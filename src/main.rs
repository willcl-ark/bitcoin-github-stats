mod db;
mod fetch;
mod github;

use chrono::NaiveDate;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "gh-stats", about = "GitHub repository stats collector")]
struct Cli {
    /// Path to SQLite database
    #[arg(long, default_value = "gh-stats.db")]
    db: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch data for a single day
    FetchDay {
        /// Date to fetch (defaults to today)
        #[arg(long)]
        date: Option<String>,
    },
    /// Backfill historical data
    Backfill {
        /// Start date (defaults to 2025-03-01)
        #[arg(long)]
        from: Option<String>,
        /// End date (defaults to today)
        #[arg(long)]
        to: Option<String>,
    },
}

fn parse_date(s: &str) -> Result<NaiveDate, Box<dyn std::error::Error>> {
    Ok(NaiveDate::parse_from_str(s, "%Y-%m-%d")?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let conn = db::open(&cli.db)?;
    let client = github::build_client()?;

    match cli.command {
        Command::FetchDay { date } => {
            let date = match date {
                Some(d) => parse_date(&d)?,
                None => chrono::Utc::now().date_naive(),
            };
            eprintln!("fetching data for {date}");

            fetch::runs::fetch_day(&client, &conn, date).await?;
            fetch::pulls::fetch_day(&client, &conn, date).await?;
            fetch::issues::fetch_day(&client, &conn, date).await?;
            fetch::commits::fetch_day(&client, &conn, date).await?;

            eprintln!("done");
        }
        Command::Backfill { from, to } => {
            let from = match from {
                Some(d) => parse_date(&d)?,
                None => parse_date("2025-03-01")?,
            };
            let to = match to {
                Some(d) => parse_date(&d)?,
                None => chrono::Utc::now().date_naive(),
            };
            eprintln!("backfilling {from} to {to}");

            fetch::runs::backfill(&client, &conn, from, to).await?;
            fetch::pulls::backfill(&client, &conn, from, to).await?;
            fetch::issues::backfill(&client, &conn, from, to).await?;
            fetch::commits::backfill(&client, &conn, from, to).await?;

            eprintln!("backfill complete");
        }
    }

    Ok(())
}
