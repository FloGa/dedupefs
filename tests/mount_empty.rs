use std::error::Error;

use assert_fs::TempDir;
use assert_fs::prelude::*;
use crazy_deduper::HashingAlgorithm;
use dedupefs::{DedupeFS, DedupeReverseFS};

#[test]
fn empty_mirror() -> Result<(), Box<dyn Error>> {
    let tempdir = TempDir::new()?;

    let mirror = tempdir.child("mirror");
    mirror.create_dir_all()?;

    let mountpoint = tempdir.child("mountpoint");
    mountpoint.create_dir_all()?;

    let mountpoint_reverse = tempdir.child("mountpoint_reverse");
    mountpoint_reverse.create_dir_all()?;

    let cache_file = tempdir.child("cache.json");

    let filesystem = DedupeFS::new(&mirror, vec![&cache_file], HashingAlgorithm::MD5, 3);
    let _session = filesystem.mount(&mountpoint)?;

    let filesystem_reverse = DedupeReverseFS::new(&mountpoint, vec![&cache_file], 3);
    let _session_reverse = filesystem_reverse.mount(&mountpoint_reverse)?;

    assert!(!mountpoint.child("data").exists(), "Empty dir is not empty");

    assert!(
        mountpoint.child(cache_file.file_name().unwrap()).exists(),
        "Cache does not exist: {:?}",
        mountpoint
            .child(cache_file.file_name().unwrap())
            .to_path_buf()
    );
    assert_eq!(
        std::fs::read(mountpoint.child(cache_file.file_name().unwrap()))?,
        std::fs::read(cache_file)?,
        "Caches do not match"
    );

    assert!(
        !dir_diff::is_different(&mirror, &mountpoint_reverse).unwrap(),
        "Source and reverse mount are different"
    );

    Ok(())
}
