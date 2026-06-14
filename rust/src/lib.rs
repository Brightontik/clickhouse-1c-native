pub mod click_house_component;
pub mod cache;
pub mod utils;
pub mod metrics;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::raw::c_long;

use native_api_1c::native_api_1c_core::ffi::{self, AttachType};
use native_api_1c::native_api_1c_core::ffi::string_utils::{from_os_string, get_str};


use crate::click_house_component::ClickHouse;

pub static mut PLATFORM_CAPABILITIES: c_long = -1;

fn log_message(message: &str) {
    let _ = OpenOptions::new()
        .create(true)
        .append(true)
        .open("C:\\tmp\\click_house_component.log")
        .and_then(|mut file| writeln!(file, "{}", message));
}


#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn GetPlatformCapabilities() -> *mut c_long {
    log_message("GetPlatformCapabilities called");
    let ptr = unsafe { &mut PLATFORM_CAPABILITIES as *mut c_long };
    let value = unsafe { *ptr };
    log_message(&format!(
        "GetPlatformCapabilities ptr={:p} value={} (0x{:x})",
        ptr, value, value as u32
    ));
    ptr
}



