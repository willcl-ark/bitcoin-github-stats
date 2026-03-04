use chrono::NaiveDate;
use octocrab::Octocrab;
use rusqlite::Connection;

use crate::db;
use crate::github;

const FETCH_CURSOR_KEY: &str = "issues:fetch_day:last_updated_at";
const BACKFILL_CURSOR_KEY: &str = "issues:backfill:page";

pub async fn fetch_day(
    client: &Octocrab,
    conn: &Connection,
    date: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let date_str = date.format("%Y-%m-%d").to_string();
    let day_end = format!("{date_str}T23:59:59Z");
    if let Some(cursor) = db::get_sync_cursor(conn, FETCH_CURSOR_KEY)? {
        if cursor.as_str() >= day_end.as_str() {
            eprintln!("level=info source=issues date={date_str} status=skip reason=cursor cursor={cursor}");
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

        eprintln!("level=info source=issues date={date_str} page={page} total={count}");

        if past_day || issues.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "issues", &date_str, count)?;
    db::set_sync_cursor(conn, FETCH_CURSOR_KEY, &day_end)?;
    eprintln!("level=info source=issues date={date_str} status=done records={count}");
    Ok(())
}

pub async fn backfill(
    client: &Octocrab,
    conn: &Connection,
    _from: NaiveDate,
    _to: NaiveDate,
    resume: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut page = 1u32;
    if resume {
        if let Some(cursor) = db::get_sync_cursor(conn, BACKFILL_CURSOR_KEY)? {
            if let Ok(p) = cursor.parse::<u32>() {
                page = p;
                eprintln!("level=info source=issues op=backfill resume_page={page}");
            }
        }
    }

    let mut total = 0usize;
    loop {
        github::check_rate_limit(client).await?;

        let path = format!(
            "/repos/bitcoin/bitcoin/issues?state=all&sort=created&direction=asc&per_page=100&page={page}"
        );
        let issues: Vec<serde_json::Value> = github::get_with_retry(client, &path).await?;

        if issues.is_empty() {
            break;
        }

        let tx = conn.unchecked_transaction()?;
        for issue in &issues {
            db::upsert_issue(&tx, issue)?;
            total += 1;
        }
        tx.commit()?;

        eprintln!("level=info source=issues op=backfill page={page} total={total}");
        db::set_sync_cursor(conn, BACKFILL_CURSOR_KEY, &page.to_string())?;

        if issues.len() < 100 {
            break;
        }
        page += 1;
    }

    db::log_sync(conn, "issues", "backfill", total)?;
    eprintln!("level=info source=issues op=backfill status=done records={total}");
    Ok(())
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
    async fn fetch_day_requests_second_page_when_first_has_100_items() {
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

        let mut page1 = Vec::new();
        for i in 0..100usize {
            page1.push(json!({
                "id": i as i64 + 1,
                "number": i as i64 + 1,
                "title": format!("Issue {i}"),
                "state": "open",
                "user": {"login": "alice"},
                "created_at": "2026-03-01T00:00:00Z",
                "updated_at": "2026-03-01T12:00:00Z",
                "closed_at": null,
                "comments": 0,
                "labels": []
            }));
        }

        let page1_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/repos/bitcoin/bitcoin/issues")
                .query_param("page", "1");
            then.status(200).json_body(json!(page1));
        });
        let page2_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/repos/bitcoin/bitcoin/issues")
                .query_param("page", "2");
            then.status(200).json_body(json!([]));
        });

        fetch_day(&client, &conn, date).await.unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM issues", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 100);
        assert_eq!(page1_mock.hits(), 1);
        assert_eq!(page2_mock.hits(), 1);
    }
}
