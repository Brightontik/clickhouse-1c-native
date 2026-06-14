use clickhouse::error::Error::Other;
use serde_json::Value;
use std::collections::HashMap;

pub struct EventMetric{
    pub event_type: String,
    pub event_name: String,
    pub duration_ms: i64,
    pub status: String,
    pub user: String,
    pub payload_json: String,
}

impl EventMetric {
    pub fn new(
        event_type: String,
        event_name: String,
        duration_ms: i64,
        status: String,
        user: String,
        payload_json: String,
    ) -> Self{
        Self { event_type, event_name, duration_ms, status, user, payload_json }
    }

    pub fn to_tsv_package(&self)-> String{
        let payload_value: Value = if self.payload_json.trim().is_empty(){
            serde_json::json!({})
        }else{
            serde_json::from_str(&self.payload_json).unwrap_or_else(|_| serde_json::json!({}))
        };
        
        let mut map_elements = Vec::new();
        if let Some(obj) = payload_value.as_object(){
            for(k,v) in obj{
                let val_str = match v {
                    Value::String(s)=> s.clone(),
                    other => other.to_string(),
                };
                let clean_k = k.replace('\'', "\\'");
                let clean_v = val_str.replace('\'', "\\'");
                map_elements.push(format!("'{}':'{}'", clean_k, clean_v));
            }
        }

        let ch_map_str = format!("{{{}}}", map_elements.join(","));

        let headers = "event_time\tevent_type\tevent_name\tduration_ms\tstatus\tuser\tcomment\tpayload\n";

        let clean_type = self.event_type.replace('\t', " ").replace('\n', " ");
        let clean_name = self.event_name.replace('\t', " ").replace('\n', " ");
        let clean_status = self.status.replace('\t', " ").replace('\n', " ");
        let clean_user = self.user.replace('\t', " ").replace('\n', " ");

        format!(
            "{}{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            headers,     
            "",        
            clean_type,   
            clean_name,  
            self.duration_ms, 
            clean_status, 
            clean_user,  
            "",         
            ch_map_str   
        )
    }

    pub fn prepare_metric_tsv(
        event_type: String,
        event_name: String,
        duration_ms: i64,
        status: String,
        user: String,
        payload_json: String,
    ) -> String {
        let metric = EventMetric::new(
            event_type,
            event_name,
            duration_ms,
            status,
            user,
            payload_json,
        );
        metric.to_tsv_package()
    }
}