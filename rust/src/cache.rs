use std::collections::HashMap;
use std::time::{Duration, Instant};
use serde_json::Value;

struct FlushBuffer{
    pub data: Vec<Value>,
    pub retry_count: u32,
}

struct TableBuffer{
    active: Vec<Value>,
    flush: Option<FlushBuffer>,
    last_flush_time: Instant,
}

pub struct AddInCache{
    buffers: HashMap<String, TableBuffer>,
    max_rows: usize,
    max_duration: Duration,
}

pub enum CacheStatus {
    Stored,
    NeedsNetworkFlush(String),
}

impl AddInCache{
    pub fn new(max_rows: usize, secs_timeout: u64) -> Self{
        Self { buffers: HashMap::new(), max_rows, max_duration: Duration::from_secs(secs_timeout), }
    }

    pub fn add(&mut self, table: String, mut new_raws: Vec<Value>) -> CacheStatus{
        let buffer = self.buffers.entry(table.clone()).or_insert_with(|| TableBuffer{
            active: Vec::new(),
            flush: None,
            last_flush_time: Instant::now(),
        });

        buffer.active.append(&mut new_raws);

        let is_overflow = buffer.active.len() >= self.max_rows;
        let is_timeout = buffer.last_flush_time.elapsed() >= self.max_duration;

        if (is_overflow || is_timeout) && !buffer.active.is_empty(){
            if buffer.flush.is_none(){
                let raw_data = std::mem::take(&mut buffer.active);

                buffer.flush = Some(FlushBuffer { data: raw_data, retry_count: 0, });

                buffer.last_flush_time = Instant::now();
                return CacheStatus::NeedsNetworkFlush(table);
            }   
        }
        CacheStatus::Stored
    }

    pub fn take_flush_data(&mut self, table: &str) -> Option<Vec<Value>>{
        if let Some(buffer) = self.buffers.get_mut(table){
            return buffer.flush.take().map(|flush_buffer| flush_buffer.data);
        }
        None
    }

    pub fn return_to_flush(&mut self, table: &str, data: Vec<Value>){
        if let Some(buffer) = self.buffers.get_mut(table){
            if buffer.flush.is_none(){
                buffer.flush = Some( FlushBuffer { data, retry_count: 1, })
            }
        }
    }

    pub fn flush_expired(&mut self) -> Vec<(String, Vec<Value>)>{
        let mut expired_batches = Vec::new();
        
        for(table, buffer) in self.buffers.iter_mut(){
            if !buffer.active.is_empty() && buffer.last_flush_time.elapsed() >= self.max_duration{
                if buffer.flush.is_none(){
                    let flush_data = std::mem::take(&mut buffer.active);
                    buffer.last_flush_time = std::time::Instant::now();

                    buffer.flush = Some(FlushBuffer { data: flush_data.clone(), retry_count: 0, });
                    expired_batches.push((table.clone(), flush_data));
                }
            }
        }
        expired_batches
    }    

    pub fn flush_all(&mut self) -> Vec<(String, Vec<Value>)>{
        let mut all_batches = Vec::new();

        for(table, buffer) in self.buffers.iter_mut(){
            if !buffer.active.is_empty(){
                let flush_data = std::mem::take(&mut buffer.active);
                buffer.last_flush_time = std::time::Instant::now();
                
                buffer.flush = Some(FlushBuffer { data: flush_data.clone(), retry_count: 0 });
                all_batches.push((table.clone(), flush_data));
            }
        }
        all_batches
    }
}