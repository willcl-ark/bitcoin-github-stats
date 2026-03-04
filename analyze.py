#!/usr/bin/env python3
"""Aggregate gh-stats.db into site/data.json for the GitHub Pages report."""

import sqlite3
import json
import os
from datetime import datetime, timezone
from collections import Counter, defaultdict

DB_PATH = "gh-stats.db"
OUT_DIR = "site"
OUT_FILE = os.path.join(OUT_DIR, "data.json")


def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def parse_dt(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s.replace("+00:00", "Z"), fmt)
        except ValueError:
            continue
    return None


def month_key(dt):
    return dt.strftime("%Y-%m")


def empty_month():
    return {
        "prs_opened": 0, "prs_merged": 0, "prs_closed_no_merge": 0, "prs_draft": 0,
        "issues_opened": 0, "issues_closed": 0,
        "commits": 0,
        "ci_total": 0, "ci_retries": 0,
        "ci_by_conclusion": Counter(),
        "ci_by_event": Counter(),
        "ci_by_workflow": Counter(),
        "ci_wf_conclusions": {},
        "pr_merge_times_days": [],
        "pr_close_times_days": [],
        "issue_close_times_days": [],
        "ci_durations_minutes": [],
        "pr_authors": Counter(),
        "pr_merged_authors": Counter(),
        "commit_authors": Counter(),
        "committers": Counter(),
        "issue_reporters": Counter(),
        "ci_actors": Counter(),
        "issue_labels": Counter(),
        "pr_by_dow": [0] * 7,
        "pr_by_hour": [0] * 24,
        "commit_by_dow": [0] * 7,
        "commit_by_hour": [0] * 24,
        "activity_dow_hour": [[0] * 24 for _ in range(7)],
    }


def main():
    conn = connect()
    months = defaultdict(empty_month)

    current_open_prs = conn.execute(
        "SELECT COUNT(*) FROM pull_requests WHERE state = 'open'"
    ).fetchone()[0]
    current_open_issues = conn.execute(
        "SELECT COUNT(*) FROM issues WHERE is_pull_request = 0 AND state = 'open'"
    ).fetchone()[0]

    for pr in conn.execute("SELECT * FROM pull_requests").fetchall():
        created = parse_dt(pr["created_at"])
        if created:
            mk = month_key(created)
            m = months[mk]
            m["prs_opened"] += 1
            if pr["draft"]:
                m["prs_draft"] += 1
            if pr["user_login"]:
                m["pr_authors"][pr["user_login"]] += 1
            m["pr_by_dow"][created.weekday()] += 1
            m["pr_by_hour"][created.hour] += 1
            m["activity_dow_hour"][created.weekday()][created.hour] += 1

        if pr["merged_at"]:
            merged = parse_dt(pr["merged_at"])
            if merged:
                mk = month_key(merged)
                m = months[mk]
                m["prs_merged"] += 1
                if pr["user_login"]:
                    m["pr_merged_authors"][pr["user_login"]] += 1
                if created:
                    m["pr_merge_times_days"].append(
                        round((merged - created).total_seconds() / 86400, 1)
                    )
        elif pr["state"] == "closed":
            closed = parse_dt(pr["closed_at"])
            if closed:
                mk = month_key(closed)
                m = months[mk]
                m["prs_closed_no_merge"] += 1
                if created:
                    m["pr_close_times_days"].append(
                        round((closed - created).total_seconds() / 86400, 1)
                    )

    for iss in conn.execute(
        "SELECT * FROM issues WHERE is_pull_request = 0"
    ).fetchall():
        created = parse_dt(iss["created_at"])
        if created:
            mk = month_key(created)
            m = months[mk]
            m["issues_opened"] += 1
            if iss["user_login"]:
                m["issue_reporters"][iss["user_login"]] += 1
            if iss["labels"]:
                try:
                    for label in json.loads(iss["labels"]):
                        m["issue_labels"][label] += 1
                except (json.JSONDecodeError, TypeError):
                    pass

        if iss["state"] == "closed" and iss["closed_at"]:
            closed = parse_dt(iss["closed_at"])
            if closed:
                mk = month_key(closed)
                months[mk]["issues_closed"] += 1
                if created:
                    months[mk]["issue_close_times_days"].append(
                        round((closed - created).total_seconds() / 86400, 1)
                    )

    for c in conn.execute("SELECT * FROM commits").fetchall():
        dt = parse_dt(c["date"])
        if dt:
            mk = month_key(dt)
            m = months[mk]
            m["commits"] += 1
            if c["author_login"]:
                m["commit_authors"][c["author_login"]] += 1
            if c["committer_login"]:
                m["committers"][c["committer_login"]] += 1
            m["commit_by_dow"][dt.weekday()] += 1
            m["commit_by_hour"][dt.hour] += 1
            m["activity_dow_hour"][dt.weekday()][dt.hour] += 1

    for r in conn.execute("SELECT * FROM workflow_runs").fetchall():
        dt = parse_dt(r["created_at"])
        if dt:
            mk = month_key(dt)
            m = months[mk]
            m["ci_total"] += 1
            if r["run_attempt"] and r["run_attempt"] > 1:
                m["ci_retries"] += 1
            if r["conclusion"]:
                m["ci_by_conclusion"][r["conclusion"]] += 1
            m["ci_by_event"][r["event"]] += 1
            if r["name"]:
                m["ci_by_workflow"][r["name"]] += 1
                if r["conclusion"]:
                    wfc = m["ci_wf_conclusions"]
                    wfc.setdefault(r["name"], Counter())[r["conclusion"]] += 1
            if r["actor_login"]:
                m["ci_actors"][r["actor_login"]] += 1

            start, end = parse_dt(r["run_started_at"]), parse_dt(r["updated_at"])
            if start and end and r["conclusion"] == "success":
                dur = (end - start).total_seconds() / 60
                if 0 < dur < 600:
                    m["ci_durations_minutes"].append(round(dur, 1))

    conn.close()

    serialized = {}
    for mk in sorted(months):
        m = months[mk]
        out = {}
        for k, v in m.items():
            if isinstance(v, Counter):
                out[k] = dict(v)
            elif k == "ci_wf_conclusions":
                out[k] = {wf: dict(cs) for wf, cs in v.items()}
            else:
                out[k] = v
        serialized[mk] = out

    output = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "repo": "bitcoin/bitcoin",
        "current_open_prs": current_open_prs,
        "current_open_issues": current_open_issues,
        "months": serialized,
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    raw_size = os.path.getsize(OUT_FILE)
    print(f"Wrote {OUT_FILE} ({len(months)} months, {raw_size:,} bytes)")


if __name__ == "__main__":
    main()
