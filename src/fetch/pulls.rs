use chrono::NaiveDate;
use octocrab::Octocrab;
use rusqlite::Connection;

use crate::db;
use crate::github;

const FETCH_CURSOR_KEY: &str = "pull_requests:fetch_day:last_updated_at";
const BACKFILL_CURSOR_KEY: &str = "pull_requests:backfill:last_updated_at";

pub async fn fetch_day(
    client: &Octocrab,
    conn: &Connection,
    date: NaiveDate,
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
                // This PR was last updated before our target date — stop
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
    from: NaiveDate,
    to: NaiveDate,
    resume: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut effective_from = from;
    if resume {
        if let Some(cursor) = db::get_sync_cursor(conn, BACKFILL_CURSOR_KEY)? {
            if let Some(cursor_date) = parse_cursor_date(&cursor) {
                let next = cursor_date + chrono::Duration::days(1);
                if next > effective_from {
                    effective_from = next;
                }
            }
        }
    }
    if effective_from > to {
        eprintln!("level=info source=pull_requests op=backfill status=already_covered");
        return Ok(());
    }
    fetch_range(client, conn, effective_from, to).await?;
    let final_cursor = format!("{}T23:59:59Z", to.format("%Y-%m-%d"));
    db::set_sync_cursor(conn, BACKFILL_CURSOR_KEY, &final_cursor)?;
    Ok(())
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
        let mut before_range = false;
        for pr in &prs {
            let updated = pr["updated_at"].as_str().unwrap_or("");
            if updated < format!("{from_str}T00:00:00Z").as_str() {
                before_range = true;
                break;
            }
            if updated <= format!("{to_str}T23:59:59Z").as_str() {
                db::upsert_pull_request(&tx, pr)?;
                count += 1;
            }
        }
        tx.commit()?;

        eprintln!("level=info source=pull_requests op=backfill page={page} total={count}");

        if before_range || prs.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "pull_requests", &range_key, count)?;
    eprintln!("level=info source=pull_requests op=backfill range={range_key} status=done records={count}");
    Ok(())
}

fn parse_cursor_date(cursor: &str) -> Option<NaiveDate> {
    if cursor.len() < 10 {
        return None;
    }
    NaiveDate::parse_from_str(&cursor[0..10], "%Y-%m-%d").ok()
}
