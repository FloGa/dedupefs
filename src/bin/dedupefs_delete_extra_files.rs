use anyhow::Result;
use dedupefs::cli::Cli;

fn main() -> Result<()> {
    Cli::delete_extra_files()
}
