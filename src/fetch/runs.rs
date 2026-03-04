use chrono::NaiveDate;
use octocrab::Octocrab;
use rusqlite::Connection;

use crate::db;
use crate::github;

const BACKFILL_CURSOR_KEY: &str = "workflow_runs:backfill:last_created_date";
const FRESHNESS_WINDOW_DAYS: i64 = 3;
const RETENTION_DAYS: i64 = 400;

pub async fn fetch_day(
    client: &Octocrab,
    conn: &Connection,
    date: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let date_str = date.format("%Y-%m-%d").to_string();

    let created = format!("{date_str}..{date_str}");
    let mut count = 0usize;
    let mut page = 1u32;

    loop {
        github::check_rate_limit(client).await?;

        let path = format!(
            "/repos/bitcoin/bitcoin/actions/runs?created={created}&per_page=100&page={page}"
        );
        let resp: serde_json::Value = github::get_with_retry(client, &path).await?;

        let runs = resp["workflow_runs"]
            .as_array()
            .ok_or("missing workflow_runs array")?;

        if runs.is_empty() {
            break;
        }

        let tx = conn.unchecked_transaction()?;
        for run in runs {
            db::upsert_workflow_run(&tx, run)?;
            count += 1;
        }
        tx.commit()?;

        eprintln!("level=info source=workflow_runs date={date_str} page={page} total={count}");

        if runs.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "workflow_runs", &date_str, count)?;
    eprintln!("level=info source=workflow_runs date={date_str} status=done records={count}");
    Ok(())
}

pub async fn backfill(
    client: &Octocrab,
    conn: &Connection,
    from: NaiveDate,
    to: NaiveDate,
    resume: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cursor_date = if resume {
        db::get_sync_cursor(conn, BACKFILL_CURSOR_KEY)?
            .and_then(|cursor| NaiveDate::parse_from_str(&cursor, "%Y-%m-%d").ok())
    } else {
        None
    };
    let mut date = compute_backfill_start(from, to, cursor_date, resume);

    let earliest = chrono::Utc::now().date_naive() - chrono::Duration::days(RETENTION_DAYS);
    if date < earliest {
        eprintln!(
            "level=info source=workflow_runs op=backfill skip_before={earliest} reason=retention_limit_{RETENTION_DAYS}d"
        );
        date = earliest;
    }

    if resume {
        eprintln!(
            "level=info source=workflow_runs op=backfill freshness_window_days={FRESHNESS_WINDOW_DAYS} start={date}"
        );
    }
    if date > to {
        eprintln!("level=info source=workflow_runs op=backfill status=already_covered");
        return Ok(());
    }

    while date <= to {
        fetch_day(client, conn, date).await?;
        db::set_sync_cursor(
            conn,
            BACKFILL_CURSOR_KEY,
            &date.format("%Y-%m-%d").to_string(),
        )?;
        date += chrono::Duration::days(1);
    }
    Ok(())
}

fn compute_backfill_start(
    from: NaiveDate,
    to: NaiveDate,
    cursor_date: Option<NaiveDate>,
    resume: bool,
) -> NaiveDate {
    let mut start = from;
    if resume {
        if let Some(cursor_date) = cursor_date {
            let next = cursor_date + chrono::Duration::days(1);
            if next > start {
                start = next;
            }
        }
    }
    let freshness_start = std::cmp::max(from, to - chrono::Duration::days(FRESHNESS_WINDOW_DAYS - 1));
    if start > freshness_start {
        freshness_start
    } else {
        start
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use octocrab::Octocrab;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn build_test_client(server: &MockServer) -> Octocrab {
        Octocrab::builder()
            .base_uri(server.url("/"))
            .unwrap()
            .personal_token("test-token".to_string())
            .build()
            .unwrap()
    }

    fn temp_db_path() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/tmp/gh-stats-test-{}-{nanos}.db", std::process::id())
    }

    #[tokio::test]
    async fn fetch_day_errors_when_workflow_runs_array_missing() {
        let server = MockServer::start();
        let client = build_test_client(&server);
        let conn = crate::db::open(&temp_db_path()).unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 3, 1).unwrap();

        let rate = json!({
            "resources": {
                "core": {"limit": 5000, "used": 0, "remaining": 4000, "reset": 4102444800u64},
                "search": {"limit": 30, "used": 0, "remaining": 30, "reset": 4102444800u64}
            },
            "rate": {"limit": 5000, "used": 0, "remaining": 4000, "reset": 4102444800u64}
        });
        let _rate_limit = server.mock(|when, then| {
            when.method(GET).path("/rate_limit");
            then.status(200).json_body(rate.clone());
        });
        let _runs = server.mock(|when, then| {
            when.method(GET).path("/repos/bitcoin/bitcoin/actions/runs");
            then.status(200).json_body(json!({}));
        });

        let err = fetch_day(&client, &conn, date).await;
        assert!(err.is_err());
    }

    #[test]
    fn backfill_start_applies_freshness_window() {
        let from = NaiveDate::from_ymd_opt(2026, 3, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
        let cursor = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();

        let start = compute_backfill_start(from, to, Some(cursor), true);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 3, 8).unwrap());
    }
}
