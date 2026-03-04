use octocrab::Octocrab;
use rusqlite::Connection;

use crate::db;
use crate::github;

const FETCH_CURSOR_KEY: &str = "pull_requests:fetch_day:last_updated_at";
const BACKFILL_CURSOR_KEY: &str = "pull_requests:backfill:page";

pub async fn fetch_day(
    client: &Octocrab,
    conn: &Connection,
    date: chrono::NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let date_str = date.format("%Y-%m-%d").to_string();
    let day_end = format!("{date_str}T23:59:59Z");
    if let Some(cursor) = db::get_sync_cursor(conn, FETCH_CURSOR_KEY)? {
        if cursor.as_str() >= day_end.as_str() {
            eprintln!(
                "level=info source=pull_requests date={date_str} status=skip reason=cursor cursor={cursor}"
            );
            return Ok(());
        }
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

        let tx = conn.unchecked_transaction()?;
        let mut all_before_date = true;
        for pr in &prs {
            let updated = pr["updated_at"].as_str().unwrap_or("");
            if updated < date_str.as_str() {
                all_before_date = true;
                break;
            }
            if updated.starts_with(&date_str) {
                db::upsert_pull_request(&tx, pr)?;
                count += 1;
                all_before_date = false;
            } else {
                all_before_date = false;
            }
        }
        tx.commit()?;

        eprintln!("level=info source=pull_requests date={date_str} page={page} total={count}");

        if all_before_date || prs.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "pull_requests", &date_str, count)?;
    db::set_sync_cursor(conn, FETCH_CURSOR_KEY, &day_end)?;
    eprintln!("level=info source=pull_requests date={date_str} status=done records={count}");
    Ok(())
}

pub async fn backfill(
    client: &Octocrab,
    conn: &Connection,
    _from: chrono::NaiveDate,
    _to: chrono::NaiveDate,
    resume: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut page = 1u32;
    if resume {
        if let Some(cursor) = db::get_sync_cursor(conn, BACKFILL_CURSOR_KEY)? {
            if let Ok(p) = cursor.parse::<u32>() {
                page = p;
                eprintln!(
                    "level=info source=pull_requests op=backfill resume_page={page}"
                );
            }
        }
    }

    let mut total = 0usize;
    loop {
        github::check_rate_limit(client).await?;

        let path = format!(
            "/repos/bitcoin/bitcoin/pulls?state=all&sort=created&direction=asc&per_page=100&page={page}"
        );
        let prs: Vec<serde_json::Value> = github::get_with_retry(client, &path).await?;

        if prs.is_empty() {
            break;
        }

        let tx = conn.unchecked_transaction()?;
        for pr in &prs {
            db::upsert_pull_request(&tx, pr)?;
            total += 1;
        }
        tx.commit()?;

        eprintln!(
            "level=info source=pull_requests op=backfill page={page} total={total}"
        );
        db::set_sync_cursor(conn, BACKFILL_CURSOR_KEY, &page.to_string())?;

        if prs.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "pull_requests", "backfill", total)?;
    eprintln!(
        "level=info source=pull_requests op=backfill status=done records={total}"
    );
    Ok(())
}
