use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use crazy_deduper::{HashingAlgorithm, Hydrator};
use daemonize::Daemonize;

use crate::DedupeFS;

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

        let filesystem = DedupeFS::new(source, caches, hashing_algorithm, declutter_levels);

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
        if Hydrator::new(args.source, args.cache_file).check_cache() {
            println!("Cache is OK");
            Ok(())
        } else {
            anyhow::bail!("Cache is not OK.")
        }
    }

    pub fn list_extra_files() -> Result<()> {
        env_logger::init();

        let args = CommandListExtraFiles::parse();
        for path in Hydrator::new(args.source, args.cache_file).list_extra_files(3) {
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

        for path in Hydrator::new(args.source, args.cache_file).list_extra_files(3) {
            std::fs::remove_file(path)?;
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
    fn verify_cli_list_extra_files() {
        CommandListExtraFiles::command().debug_assert()
    }

    #[test]
    fn verify_cli_delete_extra_files() {
        CommandDeleteExtraFiles::command().debug_assert()
    }
}
