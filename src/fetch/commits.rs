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
    if db::is_date_synced(conn, "commits", &date_str)? {
        eprintln!("commits: {date_str} already synced, skipping");
        return Ok(());
    }

    let since = format!("{date_str}T00:00:00Z");
    let until = format!("{date_str}T23:59:59Z");

    let mut count = 0usize;
    let mut page = 1u32;

    loop {
        github::check_rate_limit(client).await?;

        let commits: Vec<serde_json::Value> = client
            .get(
                format!(
                    "/repos/bitcoin/bitcoin/commits?since={since}&until={until}&per_page=100&page={page}"
                ),
                None::<&()>,
            )
            .await?;

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
    eprintln!("commits: {date_str} done — {count} records");
    Ok(())
}

pub async fn backfill(
    client: &Octocrab,
    conn: &Connection,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    // Chunk by month
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

        if db::is_date_synced(conn, "commits", &range_key)? {
            eprintln!("commits: {range_key} already synced, skipping");
            chunk_start = chunk_end + chrono::Duration::days(1);
            continue;
        }

        let since = format!("{}T00:00:00Z", chunk_start.format("%Y-%m-%d"));
        let until = format!("{}T23:59:59Z", chunk_end.format("%Y-%m-%d"));
        let mut count = 0usize;
        let mut page = 1u32;

        loop {
            github::check_rate_limit(client).await?;

            let commits: Vec<serde_json::Value> = client
                .get(
                    format!(
                        "/repos/bitcoin/bitcoin/commits?since={since}&until={until}&per_page=100&page={page}"
                    ),
                    None::<&()>,
                )
                .await?;

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
        eprintln!("commits: {range_key} done — {count} records");

        chunk_start = chunk_end + chrono::Duration::days(1);
    }

    Ok(())
}
