use std::error::Error;

use assert_fs::TempDir;
use assert_fs::prelude::*;
use crazy_deduper::HashingAlgorithm;
use dedupefs::{DedupeFS, DedupeReverseFS};

#[test]
fn mirror_big_files() -> Result<(), Box<dyn Error>> {
    let tempdir = TempDir::new()?;

    let origin = tempdir.child("origin");
    origin.create_dir_all()?;

    {
        let mut bytes = (0..u8::MAX).cycle();
        for file in 0..=2 {
            std::fs::write(
                origin.child(format!("file-{file}")),
                bytes.by_ref().take(1500 * 1024).collect::<Vec<_>>(),
            )?;
        }
    }

    let mountpoint = tempdir.child("mountpoint");
    mountpoint.create_dir_all()?;

    let mountpoint_reverse = tempdir.child("mountpoint_reverse");
    mountpoint_reverse.create_dir_all()?;

    let cache_for_mount = tempdir.child("cache.json");

    {
        let filesystem = DedupeFS::new(&origin, vec![&cache_for_mount], HashingAlgorithm::SHA1, 3);
        let _session = filesystem.mount(&mountpoint)?;

        assert!(
            mountpoint
                .child(cache_for_mount.file_name().unwrap())
                .exists(),
            "Cache does not exist"
        );
        assert_eq!(
            std::fs::read(mountpoint.child(cache_for_mount.file_name().unwrap()))?,
            std::fs::read(&cache_for_mount)?,
            "Caches do not match"
        );
    }

    {
        let filesystem = DedupeFS::new(&origin, vec![&cache_for_mount], HashingAlgorithm::SHA1, 3);
        let _session = filesystem.mount(&mountpoint)?;

        let filesystem_reverse = DedupeReverseFS::new(&mountpoint, vec![&cache_for_mount], 3);
        let _session_reverse = filesystem_reverse.mount(&mountpoint_reverse)?;

        assert!(
            !dir_diff::is_different(&origin, &mountpoint_reverse).unwrap(),
            "Source and reverse mount are different"
        );
    }

    Ok(())
}
