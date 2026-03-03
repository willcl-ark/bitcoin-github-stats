use chrono::NaiveDate;
use octocrab::Octocrab;
use rusqlite::Connection;

use crate::db;
use crate::github;

pub async fn fetch_day(
    client: &Octocrab,
    conn: &Connection,
    date: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let date_str = date.format("%Y-%m-%d").to_string();
    if db::is_date_synced(conn, "workflow_runs", &date_str)? {
        eprintln!("workflow_runs: {date_str} already synced, skipping");
        return Ok(());
    }

    let created = format!("{date_str}..{date_str}");
    let mut count = 0usize;
    let mut page = 1u32;

    loop {
        github::check_rate_limit(client).await?;

        let resp: serde_json::Value = client
            .get(
                format!("/repos/bitcoin/bitcoin/actions/runs?created={created}&per_page=100&page={page}"),
                None::<&()>,
            )
            .await?;

        let runs = resp["workflow_runs"]
            .as_array()
            .ok_or("missing workflow_runs array")?;

        if runs.is_empty() {
            break;
        }

        for run in runs {
            db::upsert_workflow_run(conn, run)?;
            count += 1;
        }

        eprintln!("workflow_runs: {date_str} page {page} — {count} total so far");

        if runs.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "workflow_runs", &date_str, count)?;
    eprintln!("workflow_runs: {date_str} done — {count} records");
    Ok(())
}

pub async fn backfill(
    client: &Octocrab,
    conn: &Connection,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut date = from;
    while date <= to {
        fetch_day(client, conn, date).await?;
        date += chrono::Duration::days(1);
    }
    Ok(())
}
