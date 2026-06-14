use std::{convert::identity, sync::Mutex, time::Duration};
use native_api_1c::{native_api_1c_core::ffi::connection::Connection, native_api_1c_macro::AddIn};
use clickhouse::{Client, Row, sql};
use serde::{Deserialize, Serialize};
use tokio::runtime;
use std::sync::Arc;
use crate::cache::{self, AddInCache, CacheStatus};

#[derive(Serialize, Deserialize, Row, Debug)]
struct TestRow{
    id: u32,
    name: String,
}
#[derive(AddIn)]
#[add_in_prop(name = "ClickHouse", ffi = "false")] 
pub struct ClickHouse {
    #[add_in_con]
    connection: std::sync::Arc<Option<&'static Connection>>,

    #[add_in_prop(readable, writable, name = "Version", name_ru = "Версия")]
    pub version: String,

    #[add_in_func(name = "Connect", name_ru = "Подключиться")]
    #[arg(Str)]
    #[arg(Str)]
    #[arg(Str)]
    #[returns(Bool)]
    pub connect: fn(&mut Self, String, String, String) -> bool,

    #[add_in_func(name = "Execute", name_ru = "ВыполнитьКоманду")]
    #[arg(Str)]
    #[returns(Str)]
    pub execute: fn(&Self, String) -> String,

    #[add_in_func(name = "BulkInsert", name_ru = "ПакетнаяВставка")]
    #[arg(Str)]
    #[arg(Str)]
    #[arg(Bool)]
    #[returns(Str)]
    pub bulk_insert: fn(&Self, String, String, bool) -> String,

    #[add_in_func(name = "FlushCacheManually", name_ru = "СбросКэша")]
    pub flush_cache_manually: fn(&Self),

    #[add_in_func(name = "LogMetric", name_ru = "ЛогМетрики")]
    #[arg(Str)]
    #[arg(Str)]
    #[arg(Int)]
    #[arg(Str)]
    #[arg(Str)]
    #[arg(Str)]
    #[returns(Bool)]
    pub log_metric: fn(&Self, String, String, i64, String, String, String) -> bool,

    pub cache: Arc<Mutex<AddInCache>>,
    
    pub runtime: Option<tokio::runtime::Runtime>,

    client: Option<Arc<Client>>,
}

    impl ClickHouse {
        pub fn new() -> Self {
            super::log_message("ClickHouse::new() called");
            let cache = Arc::new(Mutex::new(AddInCache::new(50000, 5)));
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .ok()
                .or_else(|| runtime::Runtime::new().ok());
            Self {
                connection: Arc::new(None),
                version: "0.1".to_string(),
                client: None,
                connect: Self::connect,
                execute: Self::execute,
                bulk_insert: Self::bulk_insert,
                flush_cache_manually: Self::flush_cache_manually,
                log_metric: Self::log_metric,
                cache,
                runtime,
            }
        }
    fn execute_sql_with_retry(&self, sql_query: &str) -> Result<(), clickhouse::error::Error>{
        let (client, rt) = match self.get_client_and_rt() {
            Ok(vals) => vals,
            Err(err_msg) => return Err(clickhouse::error::Error::BadResponse(err_msg)),
        };

        let client_for_send = std::sync::Arc::clone(&client);
        let sql_to_send = sql_query.to_string();

        rt.block_on(async move {
            crate::utils::run_with_retry(move || {
                let client_inner = std::sync::Arc::clone(&client_for_send);
                let sql_inner = sql_to_send.clone();
                async move {
                    client_inner.query(&sql_inner).execute().await
                }
            }).await
        })
    }
    pub fn get_client_and_rt(&self) -> Result<(Arc<Client>, &tokio::runtime::Runtime), String> {
        let Some(client_arc) = &self.client else {
            return Err("Ошибка компоненты: Клиент ClickHouse не инициализирован".to_string());
        };
        let Some(rt) = &self.runtime else {
            return Err("Ошибка компоненты: Tokio Runtime не инициализирован".to_string());
        };
         Ok((Arc::clone(client_arc), rt))
    }
    pub fn connect(&mut self, url: String, user: String, pass: String) -> bool {
        if self.runtime.is_none(){
            super::log_message("Ошибка: Tokio Runtime не инициализирован");
            return false;
        }

        
        let client = Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(pass)
            .with_validation(false);
        self.client = Some(Arc::new(client));
        super::log_message("Подключение к ClickHouse успешно настроено");
        true
    }

    pub fn execute(&self, query: String) -> String {
        let (client, rt) = match self.get_client_and_rt() {
            Ok(vals) => vals,
            Err(err_msg) => return err_msg,
        };
        
        let json_query = format!("{} FORMAT JSON", query.trim_end_matches(';').trim());

        let result = rt.block_on(async move{
            crate::utils::fetch_all_bytes(client, &json_query).await
        });
        match result {
            Ok(bytes) => {
                match String::from_utf8(bytes) {
                    Ok(json_text) => {
                        let clean_json = json_text.trim_end_matches('\0').trim().to_string();
                        if clean_json.is_empty() {
                            "{\"meta\": [], \"data\": [], \"rows\": 0}".to_string()
                        } else {
                            clean_json
                        }
                    },
                    Err(e) => format!("{{\"error\": \"Ошибка декодирования UTF-8: {}\"}}", e),
                }
            },
            Err(e) => format!("{{\"error\": \"Ошибка ClickHouse: {}\"}}", e),
        }
    }

    pub fn bulk_insert(&self, table: String, tsv_data: String, check_table: bool) -> String {
        let (client, rt) = match self.get_client_and_rt() {
            Ok(vals) => vals,
            Err(err_msg) => return err_msg,
        };

        if check_table{
            let client_for_check = Arc::clone(&client);
            let table_exists = rt.block_on(async {
                crate::utils::check_table_exists(client_for_check, &table).await
            });

            match table_exists {
                Ok(true) => { /* Таблица существует */ }
                Ok(false) => return format!("Ошибка: Таблица '{}' не существует в ClickHouse", table),
                Err(e) => return format!("Ошибка сети при проверке существования таблицы: {}", e),
            }
        }

        let rows = match crate::utils::parse_tsv_to_json(&tsv_data) {
            Ok(r) => r,
            Err(e) => return format!("Ошибка при разборе TSV данных из 1С: {}", e),
        };

        if rows.is_empty(){
            let expired_batches = self.cache.lock().unwrap().flush_expired();

            for (table_to_flush, flush_data) in expired_batches{
                if !flush_data.is_empty(){
                    let sql_query = crate::utils::build_insert_query(&table_to_flush, &flush_data).unwrap();
                    let client_clone = Arc::clone(&client);
                    let _ = rt.block_on(async move{
                        client_clone.query(&sql_query).execute().await
                    });
                }
            }
            return "Ok".to_string();
        }

        let cache_status = {
            let mut cache_lock = match self.cache.lock() {
                Ok(guard) => guard,
                Err(_) => return format!("Ошибка: Блокировка мьютекса кэша повреждена"),
            };
            cache_lock.add(table.clone(), rows)
        };

        if let CacheStatus::NeedsNetworkFlush(table_to_flush) = cache_status{
            let cache_clone = Arc::clone(&self.cache);

            let mut cache_lock = cache_clone.lock().unwrap();
            if let Some(flush_data) = cache_lock.take_flush_data(&table_to_flush){
                if !flush_data.is_empty(){
                    let sql_query = match crate::utils::build_insert_query(&table_to_flush, &flush_data) {
                        Ok(sql) => sql,
                        Err(e) => return format!("Ошибка при построении SQL запроса для вставки: {}", e),
                    };

                    if let Err(e) = self.execute_sql_with_retry(&sql_query) {
                        let mut cache_lock_retry = self.cache.lock().unwrap();
                        cache_lock_retry.return_to_flush(&table_to_flush, flush_data);
                        return format!("Ошибка отправки кэша в ClickHouse: {}", e);
                    }
                }
            }
        } 
        "OK".to_string()
    }

    pub fn flush_cache_manually(&self){
        if let Some(ref rt) = self.runtime {
            if let Ok(mut cache_lock) = self.cache.try_lock() {
                let all_batches = cache_lock.flush_all();
                if !all_batches.is_empty() && self.client.is_some() {
                    let client_clone = self.client.as_ref().unwrap().clone();
                    let _ = rt.block_on(async move {
                        for (table, data) in all_batches {
                            if let Ok(sql) = crate::utils::build_insert_query(&table, &data) {
                                let _ = client_clone.query(&sql).execute().await;
                            }
                        }
                    });
                }
            }
        }
    }

    pub fn log_metric(
        &self,
        event_type: String,
        event_name: String,
        duration_ms: i64,
        status: String,
        user: String,
        payload_json: String,
    ) -> bool {
        let tsv_package = crate::metrics::EventMetric::prepare_metric_tsv(
            event_type,
            event_name,
            duration_ms,
            status,
            user,
            payload_json,
        );
        let res = self.bulk_insert("system_1c.metrics".to_string(), tsv_package, false);

        &res == "OK"
    }
}

impl Drop for ClickHouse {
    fn drop(&mut self) {
        if let Some(ref rt) = self.runtime {
            if let Ok(mut cache_lock) = self.cache.try_lock() {
                let all_batches = cache_lock.flush_all();
                if !all_batches.is_empty() && self.client.is_some() {
                    let client_clone = self.client.as_ref().unwrap().clone();
                    let _ = rt.block_on(async move {
                        for (table, data) in all_batches {
                            if let Ok(sql) = crate::utils::build_insert_query(&table, &data) {
                                let _ = client_clone.query(&sql).execute().await;
                            }
                        }
                    });
                }
            }
        }
    }
}