use std::error::Error;
use std::path::PathBuf;
use std::thread::sleep;

use assert_fs::TempDir;
use assert_fs::prelude::*;
use crazy_deduper::{Deduper, HashingAlgorithm, Hydrator};
use dedupefs::DedupeFS;

#[test]
fn mirror_source() -> Result<(), Box<dyn Error>> {
    let tempdir = TempDir::new()?;

    let source = PathBuf::from("src");

    let mountpoint = tempdir.child("mountpoint");
    mountpoint.create_dir_all()?;

    let deduped = tempdir.child("deduped");
    deduped.create_dir_all()?;

    let hydrated = tempdir.child("hydrated");
    hydrated.create_dir_all()?;

    let cache_for_mount = tempdir.child("cache.json");
    let cache_for_cli = tempdir.child("cache_cli.json");

    let mut deduper = Deduper::new(
        &source,
        vec![cache_for_cli.as_ref()],
        HashingAlgorithm::SHA1,
        true,
    );
    deduper.write_chunks(&deduped.as_ref(), 3).unwrap();
    deduper.write_cache();

    {
        let filesystem = DedupeFS::new(&source, vec![&cache_for_mount], HashingAlgorithm::SHA1, 3);
        let _session = filesystem.mount(&mountpoint)?;

        assert!(
            !dir_diff::is_different(deduped.child("data"), mountpoint.child("data")).unwrap(),
            "Deduped source dirs are different"
        );

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

        Hydrator::new(mountpoint.as_ref(), vec![cache_for_cli.as_ref()])
            .restore_files(hydrated.as_ref(), 3);

        assert!(
            !dir_diff::is_different(&source, &hydrated).unwrap(),
            "Hydrated source dirs are different"
        );
    }

    {
        let modtime_before = std::fs::metadata(&cache_for_mount)?.modified().unwrap();

        sleep(std::time::Duration::from_millis(200));

        let filesystem = DedupeFS::new(&source, vec![&cache_for_mount], HashingAlgorithm::SHA1, 3);
        let _session = filesystem.mount(&mountpoint)?;

        let modtime_after = std::fs::metadata(&cache_for_mount)?.modified().unwrap();

        assert_eq!(
            modtime_before, modtime_after,
            "Unchanged cache file was modified"
        );
    }

    Ok(())
}
