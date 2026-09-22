use std::fs::File;
use std::io::Write;
use std::sync::{Mutex, OnceLock};

static LOG: OnceLock<Option<Mutex<File>>> = OnceLock::new();

pub fn init(verbose: bool) {
    LOG.get_or_init(|| {
        if !verbose {
            return None;
        }
        let dir = dirs::data_local_dir()
            .or_else(|| dirs::home_dir().map(|h| h.join(".local/share")))
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let path = dir.join("xget").join("xget.log");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        match File::create(&path) {
            Ok(f) => {
                eprintln!("  log: {}", path.display());
                Some(Mutex::new(f))
            }
            Err(_) => None,
        }
    });
}

pub fn write_line(msg: &str) {
    if let Some(Some(file)) = LOG.get() {
        if let Ok(mut f) = file.lock() {
            let now = chrono::Local::now().format("%H:%M:%S%.3f");
            let _ = writeln!(f, "[{now}] {msg}");
        }
    }
}

macro_rules! vlog {
    ($($arg:tt)*) => {
        $crate::log::write_line(&format!($($arg)*))
    };
}
pub(crate) use vlog;
