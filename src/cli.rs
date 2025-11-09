use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use crazy_deduper::{HashingAlgorithm, Hydrator};
use daemonize::Daemonize;

use crate::{DedupeFS, DedupeReverseFS, Mountable, WaitableBackgroundSession};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct CommandMain {
    /// Source directory
    source: PathBuf,

    /// Mount point
    mountpoint: PathBuf,

    /// Path to cache file
    ///
    /// Can be used multiple times. The files are read in reverse order, so they should be sorted
    /// with the most accurate ones in the beginning. The first given will be written.
    #[arg(long)]
    cache_file: Vec<PathBuf>,

    /// Hashing algorithm to use for chunk filenames
    #[arg(long, value_enum, default_value_t = HashingAlgorithmArgument::SHA1)]
    hashing_algorithm: HashingAlgorithmArgument,

    /// Stay in foreground, do not daemonize into the background
    #[arg(long, short)]
    foreground: bool,

    /// Declutter files into this many subdirectory levels
    #[arg(long, default_value_t = 3)]
    declutter_levels: usize,

    /// Reverse mode, present chunks re-hydrated
    #[arg(long)]
    reverse: bool,
}

#[derive(Debug, Parser)]
#[command(author, version, long_about = None)]
#[command(name = "dedupefs_create_cache")]
#[command(about = "Only create cache file without actually mounting.")]
struct CommandCreateCache {
    /// Source directory
    source: PathBuf,

    /// Path to cache file
    ///
    /// Can be used multiple times. The files are read in reverse order, so they should be sorted
    /// with the most accurate ones in the beginning. The first given will be written.
    #[arg(long)]
    cache_file: Vec<PathBuf>,

    /// Hashing algorithm to use for chunk filenames
    #[arg(long, value_enum, default_value_t = HashingAlgorithmArgument::SHA1)]
    hashing_algorithm: HashingAlgorithmArgument,

    /// Declutter files into this many subdirectory levels
    #[arg(long, default_value_t = 3)]
    declutter_levels: usize,
}

#[derive(Debug, Parser)]
#[command(author, version, long_about = None)]
#[command(name = "dedupefs_check_cache")]
#[command(about = "Check if cache file is valid and all chunks exist.")]
struct CommandCheckCache {
    /// Source directory to deduped files
    source: PathBuf,

    /// Path to cache file
    ///
    /// Can be used multiple times. The files are read in reverse order, so they should be sorted
    /// with the most accurate ones in the beginning. They will only be read, not written.
    #[arg(long)]
    cache_file: Vec<PathBuf>,

    /// Declutter files into this many subdirectory levels
    #[arg(long, default_value_t = 3)]
    declutter_levels: usize,
}

#[derive(Debug, Parser)]
#[command(author, version, long_about = None)]
#[command(name = "dedupefs_list_missing_chunks")]
#[command(about = "List chunks from cache files that are not present in the source directory.")]
struct CommandListMissingChunks {
    /// Source directory
    source: PathBuf,

    /// Path to cache file
    ///
    /// Can be used multiple times. The files are read in reverse order, so they should be sorted
    /// with the most accurate ones in the beginning. They will only be read, not written.
    #[arg(long)]
    cache_file: Vec<PathBuf>,

    /// Also display the reason for the missing or invalid chunk
    #[arg(long)]
    with_reason: bool,

    /// Separate file names with null character instead of newline
    #[arg(short = '0')]
    null: bool,
}

#[derive(Debug, Parser)]
#[command(author, version, long_about = None)]
#[command(name = "dedupefs_list_extra_files")]
#[command(about = "List files not present in any cache files.")]
struct CommandListExtraFiles {
    /// Source directory
    source: PathBuf,

    /// Path to cache file
    ///
    /// Can be used multiple times. The files are read in reverse order, so they should be sorted
    /// with the most accurate ones in the beginning. They will only be read, not written.
    #[arg(long)]
    cache_file: Vec<PathBuf>,

    /// Separate file names with null character instead of newline
    #[arg(short = '0')]
    null: bool,

    /// Declutter files into this many subdirectory levels
    #[arg(long, default_value_t = 3)]
    declutter_levels: usize,
}

#[derive(Debug, Parser)]
#[command(author, version, long_about = None)]
#[command(name = "dedupefs_delete_extra_files")]
#[command(about = "Delete files not present in any cache files.")]
struct CommandDeleteExtraFiles {
    /// Source directory
    source: PathBuf,

    /// Path to cache file
    ///
    /// Can be used multiple times. The files are read in reverse order, so they should be sorted
    /// with the most accurate ones in the beginning. They will only be read, not written.
    #[arg(long)]
    cache_file: Vec<PathBuf>,

    /// List deleted files
    #[arg(short)]
    verbose: bool,

    /// Force deletion
    #[arg(short)]
    force: bool,

    /// Declutter files into this many subdirectory levels
    #[arg(long, default_value_t = 3)]
    declutter_levels: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, ValueEnum)]
pub enum HashingAlgorithmArgument {
    MD5,
    SHA1,
    SHA256,
    SHA512,
}

impl From<HashingAlgorithmArgument> for HashingAlgorithm {
    fn from(value: HashingAlgorithmArgument) -> Self {
        match value {
            HashingAlgorithmArgument::MD5 => HashingAlgorithm::MD5,
            HashingAlgorithmArgument::SHA1 => HashingAlgorithm::SHA1,
            HashingAlgorithmArgument::SHA256 => HashingAlgorithm::SHA256,
            HashingAlgorithmArgument::SHA512 => HashingAlgorithm::SHA512,
        }
    }
}

enum MountModes {
    Normal(DedupeFS),
    Reverse(DedupeReverseFS),
}

impl MountModes {
    fn mount(
        self,
        mountpoint: impl AsRef<Path>,
    ) -> std::result::Result<WaitableBackgroundSession, Box<dyn std::error::Error>> {
        match self {
            MountModes::Normal(fs) => fs.mount(mountpoint),
            MountModes::Reverse(fs) => fs.mount(mountpoint),
        }
    }
}

pub struct Cli {}

impl Cli {
    pub fn main() -> Result<()> {
        env_logger::init();

        let args = CommandMain::parse();

        let source = args.source;
        let mountpoint = args.mountpoint;
        let caches = args.cache_file;
        let hashing_algorithm = args.hashing_algorithm.into();
        let declutter_levels = args.declutter_levels;
        let reverse = args.reverse;

        let filesystem = if reverse {
            MountModes::Reverse(DedupeReverseFS::new(source, caches, declutter_levels))
        } else {
            MountModes::Normal(DedupeFS::new(
                source,
                caches,
                hashing_algorithm,
                declutter_levels,
            ))
        };

        if !args.foreground {
            Daemonize::new().start().expect("Failed to daemonize.");
        }

        let session = filesystem.mount(&mountpoint).unwrap();
        session.wait();

        Ok(())
    }

    pub fn create_cache() -> Result<()> {
        env_logger::init();

        let args = CommandCreateCache::parse();

        let source = args.source;
        let caches = args.cache_file;
        let hashing_algorithm = args.hashing_algorithm.into();
        let declutter_levels = args.declutter_levels;

        let _ = DedupeFS::new(source, caches, hashing_algorithm, declutter_levels);

        Ok(())
    }

    pub fn check_cache() -> Result<()> {
        env_logger::init();

        let args = CommandCheckCache::parse();

        let declutter_levels = args.declutter_levels;

        if Hydrator::new(args.source, args.cache_file).check_cache(declutter_levels) {
            println!("Cache is OK");
            Ok(())
        } else {
            anyhow::bail!(
                "Cache is not OK. Use `dedupefs_list_missing_chunks` to find out which chunks are missing."
            )
        }
    }

    pub fn list_missing_chunks() -> Result<()> {
        env_logger::init();

        let args = CommandListMissingChunks::parse();
        for (path, reason) in Hydrator::new(args.source, args.cache_file).list_missing_chunks(3) {
            if args.null {
                if args.with_reason {
                    print!("{}: {}\0", reason, path.display());
                } else {
                    print!("{}\0", path.display());
                }
            } else {
                if args.with_reason {
                    println!("{}: {}", reason, path.display());
                } else {
                    println!("{}", path.display());
                }
            }
        }

        Ok(())
    }

    pub fn list_extra_files() -> Result<()> {
        env_logger::init();

        let args = CommandListExtraFiles::parse();

        let declutter_levels = args.declutter_levels;

        for path in Hydrator::new(args.source, args.cache_file).list_extra_files(declutter_levels) {
            if args.null {
                print!("{}\0", path.display());
            } else {
                println!("{}", path.display());
            }
        }

        Ok(())
    }

    pub fn delete_extra_files() -> Result<()> {
        env_logger::init();

        let args = CommandDeleteExtraFiles::parse();

        if !args.force {
            anyhow::bail!("Warning, this will delete data. If you are sure, use the `-f` flag.")
        }

        let declutter_levels = args.declutter_levels;

        for path in Hydrator::new(args.source, args.cache_file).list_extra_files(declutter_levels) {
            std::fs::remove_file(&path)?;
            if args.verbose {
                println!("Deleted {}", path.display());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn verify_cli_main() {
        CommandMain::command().debug_assert()
    }

    #[test]
    fn verify_cli_create_cache() {
        CommandCreateCache::command().debug_assert()
    }

    #[test]
    fn verify_cli_check_cache() {
        CommandCheckCache::command().debug_assert()
    }

    #[test]
    fn verify_cli_list_missing_chunks() {
        CommandListMissingChunks::command().debug_assert()
    }

    #[test]
    fn verify_cli_list_extra_files() {
        CommandListExtraFiles::command().debug_assert()
    }

    #[test]
    fn verify_cli_delete_extra_files() {
        CommandDeleteExtraFiles::command().debug_assert()
    }
}
