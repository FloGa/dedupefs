use anyhow::Result;
use dedupefs::cli::Cli;

fn main() -> Result<()> {
    Cli::list_extra_files()
}
