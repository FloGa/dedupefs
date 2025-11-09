use anyhow::Result;
use dedupefs::cli::Cli;

fn main() -> Result<()> {
    Cli::list_missing_chunks()
}
