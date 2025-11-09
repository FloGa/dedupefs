use assert_cmd::Command;
use predicates::prelude::*;

mod common;

#[test]
fn help_works() {
    Command::new(&*common::BIN_PATH)
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("\nUsage:"));
}

#[test]
fn correct_version() {
    let version = env!("CARGO_PKG_VERSION");

    Command::new(&*common::BIN_PATH)
        .arg("--version")
        .assert()
        .success()
        .stdout(format!("{} {}\n", &*common::BIN_NAME, version));
}
