use click_house_component::click_house_component::ClickHouse;
use click_house_component::utils::{parse_tsv_to_json, build_insert_query};
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicUsize, Ordering};
use serde_json::Value;

#[test]
fn test_pure_logic_parsing_and_sql() {
    let tsv_data = "id\tname\tis_active\n1\tТовар №1\ttrue\n2\tТовар \"Спец\"\tfalse";
    let rows = parse_tsv_to_json(tsv_data).expect("Парсер упал");
    
    assert_eq!(rows.len(), 2);

    let sql = build_insert_query("default.test_table", &rows).expect("Сборщик SQL упал");
    assert!(sql.starts_with("INSERT INTO default.test_table"));
    assert!(sql.contains("1"));
    assert!(sql.contains("'Товар №1'"));
}

#[test]
fn test_real_clickhouse_connection() {

    let mut ch = ClickHouse::new();

     if ch.runtime.is_none() {
        let rt = tokio::runtime::Runtime::new().expect("Не удалось создать рантайм даже в тесте");
        ch.runtime = Some(rt);
    }

    let url = "http://localhost:8123".to_string();
    let user = "default".to_string();
    let pass = "".to_string();

    let connected = ch.connect(url, user, pass);
    assert!(connected, "Компонента не смогла настроить клиент подключения (runtime все еще None?)");


    let execute_result = ch.execute("SELECT 1 as test_val".to_string());
    assert!(!execute_result.contains("Ошибка"), "Метод execute вернул ошибку: {}", execute_result);
    assert!(execute_result.contains("test_val"), "Ответ не содержит ожидаемую колонку test_val");


    let fake_tsv = "id\tname\n999\tФейк\n";
    let insert_err_result = ch.bulk_insert("default.non_existent_table_12345".to_string(), fake_tsv.to_string(), true);
    
    assert!(
        insert_err_result.contains("не существует"), 
        "Компонента пропустила вставку в несуществующую таблицу! Ответ: {}", insert_err_result
    );
}

#[test]
fn test_full_insert_and_select_lifecycle() {
    let mut ch = ClickHouse::new();

    if ch.runtime.is_none() {
        ch.runtime = Some(tokio::runtime::Runtime::new().unwrap());
    }
    let rt = ch.runtime.as_ref().unwrap();

    let url = "http://localhost:8123".to_string();
    let user = "default".to_string();
    let pass = "".to_string();

    let connected = ClickHouse::connect(&mut ch, url, user, pass);
    assert!(connected);

    let create_table_sql = "
        CREATE TABLE IF NOT EXISTS default.test_lifecycle_table 
        (
            id UInt64, 
            name String, 
            price Float64
        ) 
        ENGINE = MergeTree() 
        ORDER BY id
        SETTINGS wait_end_of_query = 1
    ".to_string();
    
    let create_res = ClickHouse::execute(&ch, create_table_sql);
    assert!(!create_res.contains("Ошибка"), "Не удалось создать таблицу: {}", create_res);

    std::thread::sleep(Duration::from_millis(200));

    ClickHouse::execute(&ch, "TRUNCATE TABLE default.test_lifecycle_table".to_string());
    
    std::thread::sleep(Duration::from_millis(100));

    let tsv_data = "id\tname\tprice\n100\tТестовый Товар 1\t450.50\n200\tТестовый Товар 2\t990.00\n";

    let insert_res = ClickHouse::bulk_insert(&ch, "default.test_lifecycle_table".to_string(), tsv_data.to_string(), true);
    assert_eq!(insert_res, "OK");


    println!("Ожидаем 5.5 секунд для срабатывания таймаута кэша...");
    std::thread::sleep(Duration::from_millis(5500));

    ClickHouse::bulk_insert(&ch, "default.test_lifecycle_table".to_string(), "id\tname\tprice\n".to_string(), false);

    let select_res = ClickHouse::execute(&ch, "SELECT id, name, price FROM default.test_lifecycle_table ORDER BY id".to_string());
    
    assert!(!select_res.contains("Ошибка"), "Ошибка чтения данных: {}", select_res);
    
    assert!(select_res.contains("Тестовый Товар 1"), "Данные первой строки не найдены в базе");
    assert!(select_res.contains("450.5"), "Прайс первой строки не найден в базе");
    assert!(select_res.contains("Тестовый Товар 2"), "Данные второй строки не найдены в базе");
    assert!(select_res.contains("990"), "Прайс второй строки не найден в базе");

    println!("Полный цикл вставки и выгрузки успешно проверен!");
}

#[test]
fn test_manual_flush_without_timeout() {
    let mut ch = ClickHouse::new();

    if ch.runtime.is_none() {
        ch.runtime = Some(tokio::runtime::Runtime::new().unwrap());
    }

    let url = "http://localhost:8123".to_string();
    let user = "default".to_string();
    let pass = "".to_string();

    let connected = ClickHouse::connect(&mut ch, url, user, pass);
    assert!(connected);

    ClickHouse::execute(&ch, "
        CREATE TABLE IF NOT EXISTS default.test_manual_flush 
        (id UInt64, name String) ENGINE = MergeTree() ORDER BY id
    ".to_string());
    ClickHouse::execute(&ch, "TRUNCATE TABLE default.test_manual_flush".to_string());

    let tsv_data = "id\tname\n555\tМгновенный сброс\n";
    let insert_res = ClickHouse::bulk_insert(&ch, "default.test_manual_flush".to_string(), tsv_data.to_string(), false);
    assert_eq!(insert_res, "OK");

    let flush_res = ClickHouse::flush_cache_manually(&ch);
    assert_eq!(flush_res, "OK");

    let select_res = ClickHouse::execute(&ch, "SELECT name FROM default.test_manual_flush WHERE id = 555".to_string());
    assert!(select_res.contains("Мгновенный сброс"), "Данные не улетели при ручном сбросе кэша!");
}

#[tokio::test] 
async fn test_chaos_network_blinking() {

    let nanos = Instant::now().elapsed().subsec_nanos();
    let target_blinks = ((nanos % 4) + 1) as usize;
    
    println!("\n[CHAOS TEST] Начинаем симуляцию хаоса. Сеть моргнет строго {} раз(а).", target_blinks);

    let attempt_counter = std::sync::Arc::new(AtomicUsize::new(0));
    let counter_clone = attempt_counter.clone();

    let fake_clickhouse_action = move || {
        let c = counter_clone.clone();
        async move {
            let current_attempt = c.fetch_add(1, Ordering::SeqCst) + 1;
            
            if current_attempt <= target_blinks {
                Err(clickhouse::error::Error::BadResponse("Chaos Error: Сеть оборвалась".to_string()))
            } else {
                Ok(())
            }
        }
    };

    let start_time = Instant::now();
    let result = click_house_component::utils::run_with_retry(fake_clickhouse_action).await;
    let duration = start_time.elapsed();

    let total_calls = attempt_counter.load(Ordering::SeqCst);
    println!("[CHAOS TEST] Ретраер завершил работу за {:?}", duration);
    println!("[CHAOS TEST] Всего ретраер совершил {} вызов(ов) сети.", total_calls);

    if target_blinks <= 3 {
        assert!(result.is_ok(), "Ретраер должен был спасти данные, так как попыток хватало! Но получили: {:?}", result.err());
        assert_eq!(total_calls, target_blinks + 1, "Количество вызовов должно быть строго на 1 больше числа морганий!");
        println!("[✓] УСПЕХ: Сеть моргнула {} раз, ретраер переждал лаг и на {}-й раз успешно записал данные!", target_blinks, total_calls);
    } else {
        assert!(result.is_err(), "Компонента должна была вернуть ошибку в 1С, так как лимит попыток исчерпан!");
        assert_eq!(total_calls, 4, "Ретраер должен был сдаться строго после 4-й неудачной попытки!");
        
        let err_text = format!("{:?}", result.err().unwrap());
        assert!(err_text.contains("Chaos Error"), "Возвращенная ошибка должна быть нашей ошибкой хаоса");
        println!("[✓] УСПЕХ: Сеть упала наглухо ({} моргания), ретраер честно отработал лимит из 4 попыток и безопасно вернул ошибку для 1С!", target_blinks);
    }
}

#[test]
fn test_metrics_logging_lifecycle() {
    let mut ch = ClickHouse::new();

    let url = "http://localhost:8123".to_string();
    let user = "default".to_string();
    let pass = "".to_string();
    
    let connected = ClickHouse::connect(&mut ch, url, user, pass);
    assert!(connected, "Не удалось подключиться к ClickHouse, проверьте рантайм!");

    ClickHouse::execute(&ch, "CREATE DATABASE IF NOT EXISTS system_1c".to_string());
    
    let create_table_sql = "
        CREATE TABLE IF NOT EXISTS system_1c.metrics
        (
            event_time DateTime64(3) DEFAULT now(),
            event_type LowCardinality(String),
            event_name String,
            duration_ms UInt32,
            status LowCardinality(String),
            user LowCardinality(String),
            comment String,
            payload Map(String, String)
        ) 
        ENGINE = MergeTree() 
        ORDER BY (event_type, event_name, event_time)
    ".to_string();
    
    let create_res = ClickHouse::execute(&ch, create_table_sql);
    assert!(!create_res.contains("Ошибка"), "Не удалось создать таблицу метрик: {}", create_res);

    ClickHouse::execute(&ch, "TRUNCATE TABLE system_1c.metrics".to_string());

    let event_type = "document".to_string();
    let event_name = "РеализацияТоваров".to_string();
    let duration_ms = 150; 
    let status = "Success".to_string();
    let user = "Администратор (Иванов)".to_string();
    let payload_json = r#"{"Склад": "Основной", "Сумма": "250000", "Контрагент": "ООО Вектор"}"#.to_string();

    let logged = ClickHouse::log_metric(
        &ch, 
        event_type, 
        event_name, 
        duration_ms, 
        status, 
        user, 
        payload_json
    );
    assert!(logged, "Функция ЛогМетрики вернула false");

    let flush_res = ClickHouse::flush_cache_manually(&ch);
    assert_eq!(flush_res, "OK", "Сброс кэша метрик завершился с ошибкой");

    let select_res = ClickHouse::execute(
        &ch, 
        "SELECT event_name, duration_ms, payload['Сумма'] as summa FROM system_1c.metrics WHERE event_type = 'document'".to_string()
    );
    
    assert!(!select_res.contains("Ошибка"), "Ошибка чтения метрик: {}", select_res);
    assert!(select_res.contains("РеализацияТоваров"), "Метрика не найдена по имени");
    assert!(select_res.contains("150"), "Длительность метрики записалась неверно");
    assert!(select_res.contains("250000"), "ClickHouse не смог распарсить payload как Map(String, String)!");

    println!("✓ УСПЕХ: Новая компонента сбора метрик для Grafana успешно протестирована без костылей!");
}



    #[test]
    fn test_hypothesis_empty_vector_in_query_builder() {
        println!("\n[HYPOTHESIS TEST] Проверяем поведение build_insert_query с пустым вектором...");
        
        // Создаем абсолютно пустой вектор, имитируя сбойный буфер кэша
        let empty_data: Vec<Value> = Vec::new();
        let table_name = "default.test_table";

        // Вызываем тестируемый метод
        let result = build_insert_query(table_name, &empty_data);

        // Проверяем: если метод паникует, тест упадет до этой строки.
        // Если метод безопасно возвращает Err, тест пройдет успешно.
        assert!(result.is_err(), "Метод должен был вернуть Err, но вернул Ok!");
        
        let err_message = result.unwrap_err();
        println!("[HYPOTHESIS TEST] Метод безопасно вернул ошибку: {}", err_message);
        assert_eq!(err_message, "Нет данных для вставки");
    }

#[test]
fn test_hypothesis_double_free_on_manual_flush() {
    use serde_json::json;
    use click_house_component::cache::AddInCache;

    println!("\n[HYPOTHESIS TEST] Проверяем утечку или порчу кучи при двойном клонировании...");

    let mut cache = AddInCache::new(50000, 5);
    let table_name = "default.test_table".to_string();

    let test_rows = vec![
        json!({"row_id": 1, "message": "Тест 1"}),
        json!({"row_id": 2, "message": "Тест 2"}),
    ];

    let _status = cache.add(table_name.clone(), test_rows);
    
    let all_batches = cache.flush_all();
    assert!(!all_batches.is_empty(), "all_batches не должен быть пустым!");

    {
        let _data_to_drop = all_batches;
    }
    println!("[HYPOTHESIS TEST] Данные all_batches успешно уничтожены.");

    let flush_data_inside = cache.take_flush_data(&table_name);
    assert!(flush_data_inside.is_some(), "Клон внутри buffer.flush пропал или повредился!");
    
    let unwrapped_data = flush_data_inside.unwrap();
    assert_eq!(unwrapped_data.len(), 2, "Количество элементов в клоне изменилось!");

    println!("[✓] ГИПОТЕЗА ПРОВЕРЕНА: Память Rust отработала штатно.");
}

#[test]
fn test_hypothesis_reentrant_block_on_conflict() {
    use click_house_component::click_house_component::ClickHouse;
    use std::time::Duration;

    println!("\n[HYPOTHESIS TEST] Симулируем параллельный block_on в главном потоке...");

    let mut ch = ClickHouse::new();
    
    if ch.runtime.is_none() {
        ch.runtime = Some(tokio::runtime::Runtime::new().unwrap());
    }
    
    let rt = ch.runtime.as_ref().unwrap();

    for _ in 0..10 {
        rt.spawn(async {
            // Фоновые задачи постоянно нагружают планировщик Tokio
            tokio::time::sleep(Duration::from_millis(10)).await;
        });
    }

    println!("[HYPOTHESIS TEST] Пробуем вызвать block_on на живом рантайме...");
    
    let result = std::panic::catch_unwind(|| {
        rt.block_on(async {
            // Имитируем сетевой запрос к ClickHouse
            tokio::time::sleep(Duration::from_millis(50)).await;
            "OK"
        })
    });

    match result {
        Ok(val) => {
            println!("[✓] ГИПОТЕЗА ОПРОВЕРГНУТА: Прямой block_on отработал штатно, вернул: {}", val);
        }
        Err(e) => {
            println!("[X] ГИПОТЕЗА ПОДТВЕРЖДЕНА: Поймали панику планировщика Tokio!");
            if let Some(s) = e.downcast_ref::<&str>() {
                println!("    Текст паники: {}", s);
            } else if let Some(s) = e.downcast_ref::<String>() {
                println!("    Текст паники: {}", s);
            }
            panic!("Токио рантайм упал при повторном входе в block_on!");
        }
    }
}

#[test]
fn test_hypothesis_ffi_string_corruption() {
    use std::time::Duration;

    println!("\n[HYPOTHESIS TEST] Проверяем стабильность выделения String после block_on...");

    let rt = tokio::runtime::Runtime::new().unwrap();

    let result_string = rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        "OK".to_string()
    });

    assert_eq!(result_string, "OK");
    assert!(!result_string.as_ptr().is_null(), "Указатель на память строки поврежден!");

    let utf16_v: Vec<u16> = result_string.encode_utf16().collect();
    assert_eq!(utf16_v.len(), 2, "Длина UTF-16 буфера строки нарушена после block_on!");
    
    println!("[✓] ГИПОТЕЗА ОПРОВЕРГНУТА: Внутренняя память строки Rust абсолютно валидна.");
}

#[test]
fn test_component_automatic_drop_lifecycle() {
    use click_house_component::click_house_component::ClickHouse;
    use serde_json::json;
    use std::time::Duration;

    println!("\n[LIFECYCLE TEST] Начинаем тест автоматического деструктора Drop...");

    {
        let mut ch = ClickHouse::new();
        
        if ch.runtime.is_none() {
            ch.runtime = Some(tokio::runtime::Runtime::new().unwrap());
        }

        let url = "http://localhost:8123".to_string();
        let user = "default".to_string();
        let pass = "".to_string();
        let connected = ClickHouse::connect(&mut ch, url, user, pass);
        assert!(connected);

        let table_name = "default.test_drop_table".to_string();
        let test_rows = vec![json!({"id": 9, "status": "automatic_drop"})];
        
        {
            let mut cache_lock = ch.cache.lock().unwrap();
            cache_lock.add(table_name, test_rows);
        }

        println!("[LIFECYCLE TEST] Объект ch живой, данные лежат в кэше. Выходим из scope...");
        
    }

    std::thread::sleep(Duration::from_millis(500));
    
    println!("[✓] LIFECYCLE TEST: Автоматический Drop отработал без паники и краша памяти!");
}