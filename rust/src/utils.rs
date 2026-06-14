use std::{collections::hash_map, fmt::format, sync::Arc, future::Future};
use clickhouse::{Client, query, error::Error as ChError};
use serde_json::{Error as JsonError, Map, Number, Value};
use tokio_retry2::{Retry, strategy::{ExponentialFactorBackoff, jitter}, RetryError};

pub async fn fetch_all_bytes(client: Arc<Client>, query: &str) -> Result<Vec<u8>,ChError> {
    let mut cursor = client.query(query).fetch_bytes("JSON")?;
    let mut all_bytes = Vec::new();

    while let Some(chunk) = cursor.next().await? {
        all_bytes.extend_from_slice(&chunk);
    }

    Ok(all_bytes)
}

pub fn parse_tsv_to_json(tvs_data: &str) -> Result<Vec<Value>, String>{
    let mut lines = tvs_data.lines();

    let header = match lines.next() {
        Some(h) if !h.trim().is_empty() => h,
        _ => return Err("Пустые данные или отсутствует строка заголовков (TSV)".to_string()),
    };

    let columns: Vec<String> = header.split('\t').map(|s| s.trim().to_string()).collect();
    if columns.is_empty() ||  columns.iter().all(|c| c.is_empty()) {
        return Err("Не удалось определить колонки в заголовке TSV".to_string());
    }

    let mut rows = Vec::new();

    for line in lines{
        if line.trim().is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split('\t').collect();
        let mut map = Map::new();

        for(i, col_name) in columns.iter().enumerate(){
            if  col_name.is_empty() {continue;}

            if let Some(field_val) = fields.get(i){
                let val_trimmed = field_val.trim();

                let json_val = if let Ok(num) = val_trimmed.parse::<i64>() {
                    Value::Number(num.into())
                } else if let Ok(float) = val_trimmed.parse::<f64>() {
                    if let Some(num) = Number::from_f64(float) {
                        Value::Number(num)
                    } else {
                        Value::String(val_trimmed.to_string())
                    }
                } else if val_trimmed.eq_ignore_ascii_case("true") {
                    Value::Bool(true)
                } else if val_trimmed.eq_ignore_ascii_case("false") {
                    Value::Bool(false)
                } else if val_trimmed.eq_ignore_ascii_case("null") || val_trimmed.is_empty() {
                    Value::Null
                } else {
                    Value::String(val_trimmed.trim_matches('"').to_string())
                };
                map.insert(col_name.clone(), json_val);
            }else{
                map.insert(col_name.clone(), Value::Null);
            }
        }
        rows.push(Value::Object(map));
    }
    Ok(rows)
}

pub fn build_insert_query(table: &str, data: &[Value]) -> Result<String, String>{
    if data.is_empty(){
        return Err("Нет данных для вставки".to_string());
    }

    let first_obj = data[0].as_object().ok_or("Структура элементов кэша повреждена")?;
    let columns: Vec<String> = first_obj.keys().cloned().collect();
    let columns_str = columns.join(", ");

    let mut values_part = Vec::new();

    for row in data{
        let obj = row.as_object().ok_or("Элемент батча не является JSON-объектом")?;
        let mut row_values = Vec::new();

        for col in &columns{
            match obj.get(col).unwrap_or(&Value::Null) {
                Value::Null => row_values.push("NULL".to_string()),
                Value::Bool(b) => row_values.push(if *b { "1".to_string() } else { "0".to_string() }),
                Value::Number(n) => row_values.push(n.to_string()),
                Value::String(s) => row_values.push(format!("'{}'", s.replace('\'', "''"))),
                other => row_values.push(format!("'{}'", other.to_string().replace('\'', "''"))),
            }
        }
        values_part.push(format!("({})", row_values.join(", ")));
    }
    let sql =format!("INSERT INTO {} ({}) VALUES {}", table, columns_str, values_part.join(", "));
    Ok(sql)
}

pub async fn check_table_exists(client: Arc<Client>, table_path: &str) -> Result<bool, ChError>{
    let parts: Vec<&str> = table_path.split('.').collect();

    let query = if parts.len() == 2 {
        format!(
            "SELECT count() FROM system.tables WHERE database = '{}' AND name = '{}' FORMAT TabSeparated",
            parts[0].replace('\'', "''"),
            parts[1].replace('\'', "''")
        )
    } else {
        format!(
            "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '{}' FORMAT TabSeparated",
            table_path.replace('\'', "''")
        )
    };

    let all_bytes = run_with_retry(move || {
        let client_inner = Arc::clone(&client);
        let q = query.clone();
        
        async move {
            fetch_all_bytes(client_inner, &q).await
        }
    }).await?;
    
    if let Ok(text) = String::from_utf8(all_bytes) {
        Ok(text.trim() == "1")
    } else {
        Err(ChError::BadResponse("Не удалось распарсить ответ системы метаданных как UTF-8".to_string()))
    }
}

pub async fn run_with_retry<F, Fut, O>(mut action: F) -> Result<O, ChError>
where 
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<O, ChError>>,
{
    let retry_strategy = ExponentialFactorBackoff::from_millis(100, 2.0)
        .map(jitter)
        .take(3);

    let result = Retry::spawn(retry_strategy, move || {
        let fut = action();
        async move {
            match fut.await {
                Ok(val) => Ok(val),
                Err(e) => Err(tokio_retry2::RetryError::transient(e)),
            }
        }
    })
    .await;

    match result {
        Ok(val) => Ok(val),
        Err(retry_error) => Err(retry_error),
    }
}