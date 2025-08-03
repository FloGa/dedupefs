use anyhow::Result;
use dedupefs::cli::Cli;

fn main() -> Result<()> {
    Cli::create_cache()
}
