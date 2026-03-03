use chrono::{Datelike, NaiveDate};
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
    if db::is_date_synced(conn, "issues", &date_str)? {
        eprintln!("issues: {date_str} already synced, skipping");
        return Ok(());
    }

    let since = format!("{date_str}T00:00:00Z");
    let next_day = (date + chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string();

    let mut count = 0usize;
    let mut page = 1u32;

    loop {
        github::check_rate_limit(client).await?;

        let path = format!(
            "/repos/bitcoin/bitcoin/issues?state=all&sort=updated&direction=asc&since={since}&per_page=100&page={page}"
        );
        let issues: Vec<serde_json::Value> = github::get_with_retry(client, &path).await?;

        if issues.is_empty() {
            break;
        }

        let mut past_day = false;
        for issue in &issues {
            let updated = issue["updated_at"].as_str().unwrap_or("");
            if updated >= format!("{next_day}T00:00:00Z").as_str() {
                past_day = true;
                break;
            }
            db::upsert_issue(conn, issue)?;
            count += 1;
        }

        eprintln!("issues: {date_str} page {page} — {count} total so far");

        if past_day || issues.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "issues", &date_str, count)?;
    eprintln!("issues: {date_str} done — {count} records");
    Ok(())
}

pub async fn backfill(
    client: &Octocrab,
    conn: &Connection,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    // Chunk by month using `since` parameter
    let mut chunk_start = from;
    while chunk_start <= to {
        let chunk_end = {
            let next_month = if chunk_start.month() == 12 {
                NaiveDate::from_ymd_opt(chunk_start.year() + 1, 1, 1).unwrap()
            } else {
                NaiveDate::from_ymd_opt(chunk_start.year(), chunk_start.month() + 1, 1).unwrap()
            };
            std::cmp::min(next_month - chrono::Duration::days(1), to)
        };

        let range_key = format!(
            "{}..{}",
            chunk_start.format("%Y-%m-%d"),
            chunk_end.format("%Y-%m-%d")
        );

        if db::is_date_synced(conn, "issues", &range_key)? {
            eprintln!("issues: {range_key} already synced, skipping");
            chunk_start = chunk_end + chrono::Duration::days(1);
            continue;
        }

        let since = format!("{}T00:00:00Z", chunk_start.format("%Y-%m-%d"));
        let until_str = format!("{}T23:59:59Z", chunk_end.format("%Y-%m-%d"));
        let mut count = 0usize;
        let mut page = 1u32;

        loop {
            github::check_rate_limit(client).await?;

            let path = format!(
                "/repos/bitcoin/bitcoin/issues?state=all&sort=updated&direction=asc&since={since}&per_page=100&page={page}"
            );
            let issues: Vec<serde_json::Value> = github::get_with_retry(client, &path).await?;

            if issues.is_empty() {
                break;
            }

            let mut past_range = false;
            for issue in &issues {
                let updated = issue["updated_at"].as_str().unwrap_or("");
                if updated > until_str.as_str() {
                    past_range = true;
                    break;
                }
                db::upsert_issue(conn, issue)?;
                count += 1;
            }

            eprintln!("issues: {range_key} page {page} — {count} total so far");

            if past_range || issues.len() < 100 {
                break;
            }
            page += 1;
        }

        db::log_sync(conn, "issues", &range_key, count)?;
        eprintln!("issues: {range_key} done — {count} records");

        chunk_start = chunk_end + chrono::Duration::days(1);
    }

    Ok(())
}
