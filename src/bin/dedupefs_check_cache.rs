use anyhow::Result;
use dedupefs::cli::Cli;

fn main() -> Result<()> {
    Cli::check_cache()
}
