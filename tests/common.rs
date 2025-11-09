use std::path::PathBuf;

use lazy_static::lazy_static;

lazy_static! {
    pub static ref BIN_PATH: PathBuf = assert_cmd::cargo::cargo_bin!("dedupefs").to_path_buf();
    pub static ref BIN_NAME: String = BIN_PATH.file_name().unwrap().to_str().unwrap().to_string();
}
