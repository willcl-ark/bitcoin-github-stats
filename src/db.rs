use rusqlite::{params, Connection, Result};

pub fn open(path: &str) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
    init_schema(&conn)?;
    Ok(conn)
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS workflow_runs (
            id                      INTEGER PRIMARY KEY,
            name                    TEXT NOT NULL,
            event                   TEXT NOT NULL,
            conclusion              TEXT,
            status                  TEXT NOT NULL,
            actor_login             TEXT,
            triggering_actor_login  TEXT,
            head_branch             TEXT,
            head_sha                TEXT,
            display_title           TEXT,
            run_number              INTEGER,
            run_attempt             INTEGER,
            created_at              TEXT NOT NULL,
            updated_at              TEXT NOT NULL,
            run_started_at          TEXT
        );

        CREATE TABLE IF NOT EXISTS pull_requests (
            id              INTEGER PRIMARY KEY,
            number          INTEGER NOT NULL UNIQUE,
            title           TEXT,
            state           TEXT NOT NULL,
            user_login      TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            closed_at       TEXT,
            merged_at       TEXT,
            draft           INTEGER,
            head_ref        TEXT,
            base_ref        TEXT,
            additions       INTEGER,
            deletions       INTEGER,
            changed_files   INTEGER,
            comments        INTEGER,
            review_comments INTEGER,
            commits         INTEGER
        );

        CREATE TABLE IF NOT EXISTS issues (
            id              INTEGER PRIMARY KEY,
            number          INTEGER NOT NULL UNIQUE,
            title           TEXT,
            state           TEXT NOT NULL,
            user_login      TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            closed_at       TEXT,
            comments        INTEGER,
            labels          TEXT,
            is_pull_request INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS commits (
            sha             TEXT PRIMARY KEY,
            author_login    TEXT,
            committer_login TEXT,
            message         TEXT,
            date            TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name      TEXT NOT NULL,
            date            TEXT NOT NULL,
            fetched_at      TEXT NOT NULL,
            record_count    INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sync_state (
            source          TEXT PRIMARY KEY,
            cursor          TEXT NOT NULL,
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        );
        ",
    )
}

pub fn upsert_workflow_run(conn: &Connection, r: &serde_json::Value) -> Result<()> {
    conn.execute(
        "INSERT INTO workflow_runs (
            id, name, event, conclusion, status, actor_login, triggering_actor_login,
            head_branch, head_sha, display_title, run_number, run_attempt,
            created_at, updated_at, run_started_at
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)
        ON CONFLICT(id) DO UPDATE SET
            conclusion=excluded.conclusion,
            status=excluded.status,
            updated_at=excluded.updated_at",
        params![
            r["id"].as_i64(),
            r["name"].as_str(),
            r["event"].as_str(),
            r["conclusion"].as_str(),
            r["status"].as_str(),
            r["actor"]["login"].as_str(),
            r["triggering_actor"]["login"].as_str(),
            r["head_branch"].as_str(),
            r["head_sha"].as_str(),
            r["display_title"].as_str(),
            r["run_number"].as_i64(),
            r["run_attempt"].as_i64(),
            r["created_at"].as_str(),
            r["updated_at"].as_str(),
            r["run_started_at"].as_str(),
        ],
    )?;
    Ok(())
}

pub fn upsert_pull_request(conn: &Connection, pr: &serde_json::Value) -> Result<()> {
    conn.execute(
        "INSERT INTO pull_requests (
            id, number, title, state, user_login, created_at, updated_at,
            closed_at, merged_at, draft, head_ref, base_ref,
            additions, deletions, changed_files, comments, review_comments, commits
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title, state=excluded.state, updated_at=excluded.updated_at,
            closed_at=excluded.closed_at, merged_at=excluded.merged_at, draft=excluded.draft,
            additions=excluded.additions, deletions=excluded.deletions,
            changed_files=excluded.changed_files, comments=excluded.comments,
            review_comments=excluded.review_comments, commits=excluded.commits",
        params![
            pr["id"].as_i64(),
            pr["number"].as_i64(),
            pr["title"].as_str(),
            pr["state"].as_str(),
            pr["user"]["login"].as_str(),
            pr["created_at"].as_str(),
            pr["updated_at"].as_str(),
            pr["closed_at"].as_str(),
            pr["merged_at"].as_str(),
            pr["draft"].as_bool().map(|b| b as i32),
            pr["head"]["ref"].as_str(),
            pr["base"]["ref"].as_str(),
            pr["additions"].as_i64(),
            pr["deletions"].as_i64(),
            pr["changed_files"].as_i64(),
            pr["comments"].as_i64(),
            pr["review_comments"].as_i64(),
            pr["commits"].as_i64(),
        ],
    )?;
    Ok(())
}

pub fn upsert_issue(conn: &Connection, issue: &serde_json::Value) -> Result<()> {
    let labels: Vec<&str> = issue["labels"]
        .as_array()
        .map(|arr| arr.iter().filter_map(|l| l["name"].as_str()).collect())
        .unwrap_or_default();
    let labels_json = serde_json::to_string(&labels).unwrap_or_default();
    let is_pr = issue.get("pull_request").is_some_and(|v| !v.is_null()) as i32;

    conn.execute(
        "INSERT INTO issues (
            id, number, title, state, user_login, created_at, updated_at,
            closed_at, comments, labels, is_pull_request
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title, state=excluded.state, updated_at=excluded.updated_at,
            closed_at=excluded.closed_at, comments=excluded.comments,
            labels=excluded.labels",
        params![
            issue["id"].as_i64(),
            issue["number"].as_i64(),
            issue["title"].as_str(),
            issue["state"].as_str(),
            issue["user"]["login"].as_str(),
            issue["created_at"].as_str(),
            issue["updated_at"].as_str(),
            issue["closed_at"].as_str(),
            issue["comments"].as_i64(),
            labels_json,
            is_pr,
        ],
    )?;
    Ok(())
}

pub fn upsert_commit(conn: &Connection, c: &serde_json::Value) -> Result<()> {
    let message = c["commit"]["message"]
        .as_str()
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("");

    conn.execute(
        "INSERT INTO commits (sha, author_login, committer_login, message, date)
        VALUES (?1,?2,?3,?4,?5)
        ON CONFLICT(sha) DO NOTHING",
        params![
            c["sha"].as_str(),
            c["author"]["login"].as_str(),
            c["committer"]["login"].as_str(),
            message,
            c["commit"]["author"]["date"].as_str(),
        ],
    )?;
    Ok(())
}

pub fn log_sync(conn: &Connection, table: &str, date: &str, count: usize) -> Result<()> {
    conn.execute(
        "INSERT INTO sync_log (table_name, date, fetched_at, record_count)
        VALUES (?1, ?2, datetime('now'), ?3)",
        params![table, date, count as i64],
    )?;
    Ok(())
}

pub fn get_sync_cursor(conn: &Connection, source: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT cursor FROM sync_state WHERE source=?1")?;
    let mut rows = stmt.query(params![source])?;
    if let Some(row) = rows.next()? {
        Ok(Some(row.get(0)?))
    } else {
        Ok(None)
    }
}

pub fn set_sync_cursor(conn: &Connection, source: &str, cursor: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO sync_state (source, cursor, updated_at)
         VALUES (?1, ?2, datetime('now'))
         ON CONFLICT(source) DO UPDATE SET
            cursor=excluded.cursor,
            updated_at=excluded.updated_at",
        params![source, cursor],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn upsert_workflow_run_updates_mutable_fields() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let first = json!({
            "id": 1,
            "name": "ci",
            "event": "push",
            "conclusion": null,
            "status": "in_progress",
            "actor": { "login": "alice" },
            "triggering_actor": { "login": "alice" },
            "head_branch": "master",
            "head_sha": "abc",
            "display_title": "test run",
            "run_number": 1,
            "run_attempt": 1,
            "created_at": "2026-03-01T00:00:00Z",
            "updated_at": "2026-03-01T00:01:00Z",
            "run_started_at": "2026-03-01T00:00:10Z"
        });
        upsert_workflow_run(&conn, &first).unwrap();

        let updated = json!({
            "id": 1,
            "name": "ci",
            "event": "push",
            "conclusion": "success",
            "status": "completed",
            "actor": { "login": "alice" },
            "triggering_actor": { "login": "alice" },
            "head_branch": "master",
            "head_sha": "abc",
            "display_title": "test run",
            "run_number": 1,
            "run_attempt": 1,
            "created_at": "2026-03-01T00:00:00Z",
            "updated_at": "2026-03-01T00:02:00Z",
            "run_started_at": "2026-03-01T00:00:10Z"
        });
        upsert_workflow_run(&conn, &updated).unwrap();

        let row: (String, Option<String>, String) = conn
            .query_row(
                "SELECT status, conclusion, updated_at FROM workflow_runs WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();

        assert_eq!(row.0, "completed");
        assert_eq!(row.1.as_deref(), Some("success"));
        assert_eq!(row.2, "2026-03-01T00:02:00Z");
    }

    #[test]
    fn sync_state_round_trip() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        assert_eq!(get_sync_cursor(&conn, "issues:fetch_day").unwrap(), None);
        set_sync_cursor(&conn, "issues:fetch_day", "2026-03-01T23:59:59Z").unwrap();
        assert_eq!(
            get_sync_cursor(&conn, "issues:fetch_day").unwrap().as_deref(),
            Some("2026-03-01T23:59:59Z")
        );
    }
}
