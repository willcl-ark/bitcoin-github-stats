use chrono::{Datelike, NaiveDate};
use octocrab::Octocrab;
use rusqlite::Connection;

use crate::db;
use crate::github;

const FETCH_CURSOR_KEY: &str = "commits:fetch_day:last_author_date";
const BACKFILL_CURSOR_KEY: &str = "commits:backfill:last_author_date";

pub async fn fetch_day(
    client: &Octocrab,
    conn: &Connection,
    date: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let date_str = date.format("%Y-%m-%d").to_string();
    let day_end = format!("{date_str}T23:59:59Z");
    if let Some(cursor) = db::get_sync_cursor(conn, FETCH_CURSOR_KEY)? {
        if cursor.as_str() >= day_end.as_str() {
            eprintln!("commits: {date_str} already covered by cursor {cursor}, skipping");
            return Ok(());
        }
    }

    let since = format!("{date_str}T00:00:00Z");
    let until = format!("{date_str}T23:59:59Z");

    let mut count = 0usize;
    let mut page = 1u32;

    loop {
        github::check_rate_limit(client).await?;

        let path = format!(
            "/repos/bitcoin/bitcoin/commits?since={since}&until={until}&per_page=100&page={page}"
        );
        let commits: Vec<serde_json::Value> = github::get_with_retry(client, &path).await?;

        if commits.is_empty() {
            break;
        }

        for c in &commits {
            db::upsert_commit(conn, c)?;
            count += 1;
        }

        eprintln!("commits: {date_str} page {page} — {count} total so far");

        if commits.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "commits", &date_str, count)?;
    db::set_sync_cursor(conn, FETCH_CURSOR_KEY, &day_end)?;
    eprintln!("commits: {date_str} done — {count} records");
    Ok(())
}

pub async fn backfill(
    client: &Octocrab,
    conn: &Connection,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
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
        eprintln!("commits: backfill cursor already covers requested range");
        return Ok(());
    }

    // Chunk by month
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
        let until = format!("{}T23:59:59Z", chunk_end.format("%Y-%m-%d"));
        let mut count = 0usize;
        let mut page = 1u32;

        loop {
            github::check_rate_limit(client).await?;

            let path = format!(
                "/repos/bitcoin/bitcoin/commits?since={since}&until={until}&per_page=100&page={page}"
            );
            let commits: Vec<serde_json::Value> = github::get_with_retry(client, &path).await?;

            if commits.is_empty() {
                break;
            }

            for c in &commits {
                db::upsert_commit(conn, c)?;
                count += 1;
            }

            eprintln!("commits: {range_key} page {page} — {count} total so far");

            if commits.len() < 100 {
                break;
            }
            page += 1;
        }

        db::log_sync(conn, "commits", &range_key, count)?;
        db::set_sync_cursor(conn, BACKFILL_CURSOR_KEY, &until)?;
        eprintln!("commits: {range_key} done — {count} records");

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
