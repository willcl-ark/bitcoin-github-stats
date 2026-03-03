use chrono::{Datelike, NaiveDate};
use octocrab::Octocrab;
use rusqlite::Connection;

use crate::db;
use crate::github;

const FETCH_CURSOR_KEY: &str = "issues:fetch_day:last_updated_at";
const BACKFILL_CURSOR_KEY: &str = "issues:backfill:last_updated_at";

pub async fn fetch_day(
    client: &Octocrab,
    conn: &Connection,
    date: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let date_str = date.format("%Y-%m-%d").to_string();
    let day_end = format!("{date_str}T23:59:59Z");
    if let Some(cursor) = db::get_sync_cursor(conn, FETCH_CURSOR_KEY)? {
        if cursor.as_str() >= day_end.as_str() {
            eprintln!("issues: {date_str} already covered by cursor {cursor}, skipping");
            return Ok(());
        }
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

        let tx = conn.unchecked_transaction()?;
        let mut past_day = false;
        for issue in &issues {
            let updated = issue["updated_at"].as_str().unwrap_or("");
            if updated >= format!("{next_day}T00:00:00Z").as_str() {
                past_day = true;
                break;
            }
            db::upsert_issue(&tx, issue)?;
            count += 1;
        }
        tx.commit()?;

        eprintln!("issues: {date_str} page {page} — {count} total so far");

        if past_day || issues.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "issues", &date_str, count)?;
    db::set_sync_cursor(conn, FETCH_CURSOR_KEY, &day_end)?;
    eprintln!("issues: {date_str} done — {count} records");
    Ok(())
}

pub async fn backfill(
    client: &Octocrab,
    conn: &Connection,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(cursor) = db::get_sync_cursor(conn, BACKFILL_CURSOR_KEY)? {
        if let Some(cursor_date) = parse_cursor_date(&cursor) {
            let next = cursor_date + chrono::Duration::days(1);
            if next > from {
                eprintln!("issues: resuming backfill from {next}");
            }
        }
    }

    // Chunk by month using `since` parameter
    let mut chunk_start = from;
    if let Some(cursor) = db::get_sync_cursor(conn, BACKFILL_CURSOR_KEY)? {
        if let Some(cursor_date) = parse_cursor_date(&cursor) {
            let next = cursor_date + chrono::Duration::days(1);
            if next > chunk_start {
                chunk_start = next;
            }
        }
    }
    if chunk_start > to {
        eprintln!("issues: backfill cursor already covers requested range");
        return Ok(());
    }

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

            let tx = conn.unchecked_transaction()?;
            let mut past_range = false;
            for issue in &issues {
                let updated = issue["updated_at"].as_str().unwrap_or("");
                if updated > until_str.as_str() {
                    past_range = true;
                    break;
                }
                db::upsert_issue(&tx, issue)?;
                count += 1;
            }
            tx.commit()?;

            eprintln!("issues: {range_key} page {page} — {count} total so far");

            if past_range || issues.len() < 100 {
                break;
            }
            page += 1;
        }

        db::log_sync(conn, "issues", &range_key, count)?;
        db::set_sync_cursor(conn, BACKFILL_CURSOR_KEY, &until_str)?;
        eprintln!("issues: {range_key} done — {count} records");

        chunk_start = chunk_end + chrono::Duration::days(1);
    }

    Ok(())
}

fn parse_cursor_date(cursor: &str) -> Option<NaiveDate> {
    if cursor.len() < 10 {
        return None;
    }
    NaiveDate::parse_from_str(&cursor[0..10], "%Y-%m-%d").ok()
}
