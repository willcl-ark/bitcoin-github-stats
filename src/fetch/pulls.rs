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
    if db::is_date_synced(conn, "pull_requests", &date_str)? {
        eprintln!("pull_requests: {date_str} already synced, skipping");
        return Ok(());
    }

    let mut count = 0usize;
    let mut page = 1u32;

    loop {
        github::check_rate_limit(client).await?;

        let path = format!(
            "/repos/bitcoin/bitcoin/pulls?state=all&sort=updated&direction=desc&per_page=100&page={page}"
        );
        let prs: Vec<serde_json::Value> = github::get_with_retry(client, &path).await?;

        if prs.is_empty() {
            break;
        }

        let mut all_before_date = true;
        for pr in &prs {
            let updated = pr["updated_at"].as_str().unwrap_or("");
            if updated < date_str.as_str() {
                // This PR was last updated before our target date — stop
                all_before_date = true;
                break;
            }
            if updated.starts_with(&date_str) {
                db::upsert_pull_request(conn, pr)?;
                count += 1;
                all_before_date = false;
            } else {
                all_before_date = false;
            }
        }

        eprintln!("pull_requests: {date_str} page {page} — {count} total so far");

        if all_before_date || prs.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "pull_requests", &date_str, count)?;
    eprintln!("pull_requests: {date_str} done — {count} records");
    Ok(())
}

pub async fn backfill(
    client: &Octocrab,
    conn: &Connection,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    fetch_range(client, conn, from, to).await
}

async fn fetch_range(
    client: &Octocrab,
    conn: &Connection,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let from_str = from.format("%Y-%m-%d").to_string();
    let to_str = to.format("%Y-%m-%d").to_string();

    // For backfill, paginate by updated_at descending and collect PRs in range
    let range_key = format!("{from_str}..{to_str}");
    if db::is_date_synced(conn, "pull_requests", &range_key)? {
        eprintln!("pull_requests: {range_key} already synced, skipping");
        return Ok(());
    }

    let mut count = 0usize;
    let mut page = 1u32;

    loop {
        github::check_rate_limit(client).await?;

        let path = format!(
            "/repos/bitcoin/bitcoin/pulls?state=all&sort=updated&direction=desc&per_page=100&page={page}"
        );
        let prs: Vec<serde_json::Value> = github::get_with_retry(client, &path).await?;

        if prs.is_empty() {
            break;
        }

        let mut before_range = false;
        for pr in &prs {
            let updated = pr["updated_at"].as_str().unwrap_or("");
            if updated < format!("{from_str}T00:00:00Z").as_str() {
                before_range = true;
                break;
            }
            if updated <= format!("{to_str}T23:59:59Z").as_str() {
                db::upsert_pull_request(conn, pr)?;
                count += 1;
            }
        }

        eprintln!("pull_requests: backfill page {page} — {count} total so far");

        if before_range || prs.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "pull_requests", &range_key, count)?;
    eprintln!("pull_requests: backfill {range_key} done — {count} records");
    Ok(())
}
