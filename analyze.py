#!/usr/bin/env python3
"""
Bitcoin Core GitHub Statistics Report
Analyzes 12 months of scraped data from bitcoin/bitcoin.
Outputs a single self-contained HTML file with interactive Chart.js charts.
"""

import sqlite3
import json
from datetime import datetime, timedelta
from collections import Counter
from html import escape

import numpy as np

DB_PATH = "gh-stats.db"
OUT_FILE = "report.html"
CUTOFF = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")

COLORS = {
    "blue": "#1f77b4",
    "orange": "#ff7f0e",
    "green": "#2ca02c",
    "red": "#d62728",
    "purple": "#9467bd",
    "brown": "#8c564b",
    "pink": "#e377c2",
    "gray": "#7f7f7f",
}

CHART_ID = 0


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


def chart(spec):
    global CHART_ID
    CHART_ID += 1
    cid = f"chart_{CHART_ID}"
    return (
        f'<div class="chart-wrap"><canvas id="{cid}"></canvas></div>\n'
        f"<script>new Chart(document.getElementById('{cid}'), {json.dumps(spec)});</script>\n"
    )


def histogram_bins(values, n_bins=50, cap=None):
    if cap is not None:
        values = [min(v, cap) for v in values]
    arr = np.array(values)
    counts, edges = np.histogram(arr, bins=n_bins)
    labels = [f"{edges[i]:.0f}-{edges[i+1]:.0f}" for i in range(len(counts))]
    return labels, counts.tolist()


# ──────────────────────────────────────────────
# HTML helpers
# ──────────────────────────────────────────────

def h2(title):
    return f"<h2>{escape(title)}</h2>\n"


def stat_row(label, value):
    return f"<tr><td>{escape(label)}</td><td><strong>{escape(str(value))}</strong></td></tr>\n"


def stats_table(rows):
    html = '<table class="stats">\n'
    for label, value in rows:
        html += stat_row(label, value)
    html += "</table>\n"
    return html


def ranking_table(title, headers, rows):
    html = f"<h3>{escape(title)}</h3>\n"
    html += '<table class="ranking">\n<tr>'
    for h in headers:
        html += f"<th>{escape(h)}</th>"
    html += "</tr>\n"
    for row in rows:
        html += "<tr>"
        for cell in row:
            html += f"<td>{escape(str(cell))}</td>"
        html += "</tr>\n"
    html += "</table>\n"
    return html


def heatmap_table(data, row_labels, col_labels, title):
    max_val = max(max(row) for row in data) or 1
    html = f"<h3>{escape(title)}</h3>\n"
    html += '<table class="heatmap"><tr><th></th>'
    for c in col_labels:
        html += f"<th>{escape(str(c))}</th>"
    html += "</tr>\n"
    for i, row_label in enumerate(row_labels):
        html += f"<tr><td class='hm-label'>{escape(str(row_label))}</td>"
        for j, val in enumerate(data[i]):
            intensity = val / max_val
            r = int(255 - intensity * (255 - 215))
            g = int(255 - intensity * (255 - 48))
            b = int(255 - intensity * (255 - 39))
            bg = f"rgb({r},{g},{b})"
            fg = "#fff" if intensity > 0.5 else "#333"
            html += (
                f'<td class="hm-cell" style="background:{bg};color:{fg}" '
                f'title="{row_label} {col_labels[j]}:00 UTC — {int(val)} events">'
                f"{int(val)}</td>"
            )
        html += "</tr>\n"
    html += "</table>\n"
    return html


# ──────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────

def load_prs(conn):
    return conn.execute("SELECT * FROM pull_requests WHERE created_at >= ?", (CUTOFF,)).fetchall()

def load_issues(conn):
    return conn.execute("SELECT * FROM issues WHERE is_pull_request = 0 AND created_at >= ?", (CUTOFF,)).fetchall()

def load_commits(conn):
    return conn.execute("SELECT * FROM commits WHERE date >= ? ORDER BY date", (CUTOFF,)).fetchall()

def load_runs(conn):
    return conn.execute("SELECT * FROM workflow_runs WHERE created_at >= ?", (CUTOFF,)).fetchall()


# ──────────────────────────────────────────────
# Report sections
# ──────────────────────────────────────────────

def report_overview(conn):
    html = h2("1. Overview")
    date_col = {"commits": "date"}
    counts = {}
    for tbl in ("workflow_runs", "pull_requests", "issues", "commits"):
        col = date_col.get(tbl, "created_at")
        counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {col} >= ?", (CUTOFF,)).fetchone()[0]

    issue_count = conn.execute(
        "SELECT COUNT(*) FROM issues WHERE is_pull_request = 0 AND created_at >= ?", (CUTOFF,)
    ).fetchone()[0]
    pr_range = conn.execute(
        "SELECT MIN(created_at), MAX(created_at) FROM pull_requests WHERE created_at >= ?", (CUTOFF,)
    ).fetchone()
    unique_pr = conn.execute(
        "SELECT COUNT(DISTINCT user_login) FROM pull_requests WHERE created_at >= ?", (CUTOFF,)
    ).fetchone()[0]
    unique_commit = conn.execute(
        "SELECT COUNT(DISTINCT author_login) FROM commits WHERE author_login IS NOT NULL AND date >= ?", (CUTOFF,)
    ).fetchone()[0]

    html += stats_table([
        ("Total workflow runs", f"{counts['workflow_runs']:,}"),
        ("Total pull requests", f"{counts['pull_requests']:,}"),
        ("Total issues (excluding PRs)", f"{issue_count:,}"),
        ("Total commits", f"{counts['commits']:,}"),
        ("Data range (PRs)", f"{pr_range[0][:10]} to {pr_range[1][:10]}"),
        ("Unique PR authors", str(unique_pr)),
        ("Unique commit authors", str(unique_commit)),
    ])
    return html


def report_pr_activity(prs):
    html = h2("2. Pull Request Activity")

    merged = [r for r in prs if r["merged_at"]]
    closed_no_merge = [r for r in prs if r["state"] == "closed" and not r["merged_at"]]
    open_prs = [r for r in prs if r["state"] == "open"]

    total_closed = len(merged) + len(closed_no_merge)
    merge_rate = len(merged) / total_closed * 100 if total_closed else 0

    merge_days = []
    for pr in merged:
        created, m = parse_dt(pr["created_at"]), parse_dt(pr["merged_at"])
        if created and m:
            merge_days.append((m - created).total_seconds() / 86400)

    close_days = []
    for pr in closed_no_merge:
        created, closed = parse_dt(pr["created_at"]), parse_dt(pr["closed_at"])
        if created and closed:
            close_days.append((closed - created).total_seconds() / 86400)

    drafts = sum(1 for pr in prs if pr["draft"])

    rows = [
        ("PRs merged", f"{len(merged):,}"),
        ("PRs closed without merge", f"{len(closed_no_merge):,}"),
        ("PRs currently open", f"{len(open_prs):,}"),
        ("Merge rate (merged / all closed)", f"{merge_rate:.1f}%"),
        ("Draft PRs", f"{drafts:,}"),
    ]
    if merge_days:
        arr = np.array(merge_days)
        rows += [
            ("Median time to merge (days)", f"{np.median(arr):.1f}"),
            ("Mean time to merge (days)", f"{np.mean(arr):.1f}"),
            ("90th percentile time to merge (days)", f"{np.percentile(arr, 90):.1f}"),
            ("Fastest merge (hours)", f"{np.min(arr)*24:.1f}"),
        ]
    if close_days:
        rows.append(("Median time to close without merge (days)", f"{np.median(close_days):.1f}"))
    html += stats_table(rows)

    # Chart: PRs opened/merged per month
    opened_by_month = Counter()
    merged_by_month = Counter()
    for pr in prs:
        dt = parse_dt(pr["created_at"])
        if dt:
            opened_by_month[month_key(dt)] += 1
    for pr in merged:
        dt = parse_dt(pr["merged_at"])
        if dt:
            merged_by_month[month_key(dt)] += 1

    months = sorted(set(opened_by_month) | set(merged_by_month))
    html += chart({
        "type": "bar",
        "data": {
            "labels": months,
            "datasets": [
                {"label": "Opened", "data": [opened_by_month[m] for m in months],
                 "backgroundColor": COLORS["blue"]},
                {"label": "Merged", "data": [merged_by_month[m] for m in months],
                 "backgroundColor": COLORS["green"]},
            ],
        },
        "options": {"plugins": {"title": {"display": True, "text": "Pull Requests Opened vs Merged per Month"}}},
    })

    # Chart: Time to merge distribution
    if merge_days:
        labels, counts = histogram_bins(merge_days, 40, cap=365)
        html += chart({
            "type": "bar",
            "data": {
                "labels": labels,
                "datasets": [{"label": "PRs", "data": counts, "backgroundColor": COLORS["blue"]}],
            },
            "options": {
                "plugins": {"title": {"display": True, "text": "Time to Merge Distribution (capped at 365 days)"}},
                "scales": {"x": {"ticks": {"maxTicksLimit": 15}}},
            },
        })

    # Chart: PR state breakdown
    html += chart({
        "type": "doughnut",
        "data": {
            "labels": ["Merged", "Closed (no merge)", "Open"],
            "datasets": [{"data": [len(merged), len(closed_no_merge), len(open_prs)],
                          "backgroundColor": [COLORS["green"], COLORS["red"], COLORS["blue"]]}],
        },
        "options": {"plugins": {"title": {"display": True, "text": "Pull Request State Breakdown"}}},
    })

    # Chart: Median merge time trend with IQR
    monthly_merge_times = {}
    for pr in merged:
        created, m = parse_dt(pr["created_at"]), parse_dt(pr["merged_at"])
        if created and m:
            mk = month_key(m)
            monthly_merge_times.setdefault(mk, []).append(
                (m - created).total_seconds() / 86400
            )
    months_mt = sorted(monthly_merge_times)
    if months_mt:
        medians = [round(float(np.median(monthly_merge_times[m])), 1) for m in months_mt]
        p25 = [round(float(np.percentile(monthly_merge_times[m], 25)), 1) for m in months_mt]
        p75 = [round(float(np.percentile(monthly_merge_times[m], 75)), 1) for m in months_mt]
        html += chart({
            "type": "line",
            "data": {
                "labels": months_mt,
                "datasets": [
                    {"label": "75th percentile", "data": p75,
                     "backgroundColor": "rgba(31,119,180,0.1)", "borderColor": "transparent",
                     "fill": True, "pointRadius": 0},
                    {"label": "Median", "data": medians,
                     "borderColor": COLORS["blue"], "backgroundColor": "rgba(31,119,180,0.1)",
                     "fill": "-1", "tension": 0.3},
                    {"label": "25th percentile", "data": p25,
                     "borderColor": "transparent", "backgroundColor": "transparent",
                     "fill": False, "pointRadius": 0},
                ],
            },
            "options": {
                "plugins": {"title": {"display": True, "text": "Median Time to Merge Trend (with IQR)"}},
                "scales": {"y": {"title": {"display": True, "text": "Days"}}},
            },
        })

    return html


def report_pr_authors(prs):
    html = h2("3. Pull Request Authors")

    author_counts = Counter(pr["user_login"] for pr in prs if pr["user_login"])
    top20 = author_counts.most_common(20)

    html += ranking_table("Top 20 PR Authors", ["#", "Author", "PRs"],
        [(i, u, f"{c:,}") for i, (u, c) in enumerate(top20, 1)])

    merged_authors = Counter(
        pr["user_login"] for pr in prs if pr["merged_at"] and pr["user_login"]
    )
    top_merged = merged_authors.most_common(10)
    html += ranking_table("Top 10 Authors by Merged PRs", ["#", "Author", "Merged"],
        [(i, u, f"{c:,}") for i, (u, c) in enumerate(top_merged, 1)])

    first_pr = {}
    for pr in prs:
        user, dt = pr["user_login"], parse_dt(pr["created_at"])
        if user and dt:
            mk = month_key(dt)
            if user not in first_pr or mk < first_pr[user]:
                first_pr[user] = mk
    new_per_month = Counter(first_pr.values())

    authors_per_month = {}
    for pr in prs:
        user, dt = pr["user_login"], parse_dt(pr["created_at"])
        if user and dt:
            authors_per_month.setdefault(month_key(dt), set()).add(user)

    months_a = sorted(authors_per_month)
    rows = []
    if new_per_month:
        rows.append(("Avg new PR authors per month", f"{np.mean(list(new_per_month.values())):.1f}"))
    if months_a:
        rows.append(("Avg unique PR authors per month",
                      f"{np.mean([len(authors_per_month[m]) for m in months_a]):.1f}"))
    html += stats_table(rows)

    # Chart: Top 20 PR authors (horizontal bar)
    users = [u for u, _ in top20][::-1]
    counts = [c for _, c in top20][::-1]
    html += chart({
        "type": "bar",
        "data": {
            "labels": users,
            "datasets": [{"label": "PRs", "data": counts, "backgroundColor": COLORS["blue"]}],
        },
        "options": {
            "indexAxis": "y",
            "plugins": {"title": {"display": True, "text": "Top 20 PR Authors"}},
        },
    })

    # Chart: Author diversity (unique + new per month)
    if months_a:
        nm = sorted(new_per_month)
        html += chart({
            "type": "bar",
            "data": {
                "labels": months_a,
                "datasets": [
                    {"label": "Unique authors", "data": [len(authors_per_month[m]) for m in months_a],
                     "backgroundColor": COLORS["blue"]},
                    {"label": "New (first-time) authors", "data": [new_per_month.get(m, 0) for m in months_a],
                     "backgroundColor": COLORS["green"]},
                ],
            },
            "options": {"plugins": {"title": {"display": True, "text": "PR Author Diversity per Month"}}},
        })

    return html


def report_pr_timing(prs):
    html = h2("4. Pull Request Timing Patterns")

    dow_counts = Counter()
    hour_counts = Counter()
    for pr in prs:
        dt = parse_dt(pr["created_at"])
        if dt:
            dow_counts[dt.weekday()] += 1
            hour_counts[dt.hour] += 1

    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    weekend = dow_counts.get(5, 0) + dow_counts.get(6, 0)
    weekday = sum(dow_counts.get(i, 0) for i in range(5))
    peak_hour = max(hour_counts, key=hour_counts.get) if hour_counts else 0

    html += stats_table([
        ("Weekend vs weekday PR ratio", f"{weekend/(weekday or 1)*100:.1f}% of weekday volume"),
        ("Peak hour for PR creation (UTC)", f"{peak_hour}:00"),
    ])

    html += chart({
        "type": "bar",
        "data": {
            "labels": days,
            "datasets": [{"label": "PRs opened", "data": [dow_counts.get(i, 0) for i in range(7)],
                          "backgroundColor": COLORS["blue"]}],
        },
        "options": {"plugins": {"title": {"display": True, "text": "PRs Opened by Day of Week"}}},
    })

    html += chart({
        "type": "bar",
        "data": {
            "labels": [f"{h}:00" for h in range(24)],
            "datasets": [{"label": "PRs opened", "data": [hour_counts.get(h, 0) for h in range(24)],
                          "backgroundColor": COLORS["orange"]}],
        },
        "options": {"plugins": {"title": {"display": True, "text": "PRs Opened by Hour (UTC)"}}},
    })

    return html


def report_issues(issues):
    html = h2("5. Issues")

    open_issues = [i for i in issues if i["state"] == "open"]
    closed_issues = [i for i in issues if i["state"] == "closed"]

    close_days = []
    for iss in closed_issues:
        created, closed = parse_dt(iss["created_at"]), parse_dt(iss["closed_at"])
        if created and closed:
            close_days.append((closed - created).total_seconds() / 86400)

    rows = [
        ("Total issues (non-PR)", f"{len(issues):,}"),
        ("Open issues", f"{len(open_issues):,}"),
        ("Closed issues", f"{len(closed_issues):,}"),
    ]
    if close_days:
        arr = np.array(close_days)
        rows += [
            ("Median time to close (days)", f"{np.median(arr):.1f}"),
            ("Mean time to close (days)", f"{np.mean(arr):.1f}"),
        ]
    html += stats_table(rows)

    all_labels = []
    for iss in issues:
        raw = iss["labels"]
        if raw:
            try:
                all_labels.extend(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                pass

    label_counts = Counter(all_labels)
    top_labels = label_counts.most_common(20)
    html += ranking_table("Top 20 Issue Labels", ["#", "Label", "Count"],
        [(i, l, f"{c:,}") for i, (l, c) in enumerate(top_labels, 1)])

    reporter_counts = Counter(iss["user_login"] for iss in issues if iss["user_login"])
    top_reporters = reporter_counts.most_common(15)
    html += ranking_table("Top 15 Issue Reporters", ["#", "Author", "Issues"],
        [(i, u, f"{c:,}") for i, (u, c) in enumerate(top_reporters, 1)])

    # Chart: Issues opened/closed per month
    opened_by_month = Counter()
    closed_by_month = Counter()
    for iss in issues:
        dt = parse_dt(iss["created_at"])
        if dt:
            opened_by_month[month_key(dt)] += 1
    for iss in closed_issues:
        dt = parse_dt(iss["closed_at"])
        if dt:
            closed_by_month[month_key(dt)] += 1

    months = sorted(set(opened_by_month) | set(closed_by_month))
    html += chart({
        "type": "bar",
        "data": {
            "labels": months,
            "datasets": [
                {"label": "Opened", "data": [opened_by_month[m] for m in months],
                 "backgroundColor": COLORS["blue"]},
                {"label": "Closed", "data": [closed_by_month[m] for m in months],
                 "backgroundColor": COLORS["green"]},
            ],
        },
        "options": {"plugins": {"title": {"display": True, "text": "Issues Opened vs Closed per Month"}}},
    })

    # Chart: Issue labels (horizontal bar)
    if top_labels:
        lab = [l for l, _ in top_labels][::-1]
        cnt = [c for _, c in top_labels][::-1]
        html += chart({
            "type": "bar",
            "data": {
                "labels": lab,
                "datasets": [{"label": "Issues", "data": cnt, "backgroundColor": COLORS["purple"]}],
            },
            "options": {
                "indexAxis": "y",
                "plugins": {"title": {"display": True, "text": "Top 20 Issue Labels"}},
            },
        })

    # Chart: Issue close time distribution
    if close_days:
        labels, counts = histogram_bins(close_days, 40, cap=365)
        html += chart({
            "type": "bar",
            "data": {
                "labels": labels,
                "datasets": [{"label": "Issues", "data": counts, "backgroundColor": COLORS["green"]}],
            },
            "options": {
                "plugins": {"title": {"display": True, "text": "Time to Close Issues (capped at 365 days)"}},
                "scales": {"x": {"ticks": {"maxTicksLimit": 15}}},
            },
        })

    # Chart: Issue backlog (cumulative open)
    events = []
    for iss in issues:
        dt = parse_dt(iss["created_at"])
        if dt:
            events.append((dt, 1))
        if iss["closed_at"]:
            cdt = parse_dt(iss["closed_at"])
            if cdt:
                events.append((cdt, -1))
    events.sort()
    if events:
        cumulative, running, dates = [], 0, []
        for dt, delta in events:
            running += delta
            dates.append(dt.strftime("%Y-%m-%d"))
            cumulative.append(running)

        html += chart({
            "type": "line",
            "data": {
                "labels": dates,
                "datasets": [{
                    "label": "Open issues",
                    "data": cumulative,
                    "borderColor": COLORS["blue"],
                    "backgroundColor": "rgba(31,119,180,0.1)",
                    "fill": True,
                    "pointRadius": 0,
                    "tension": 0.3,
                }],
            },
            "options": {
                "plugins": {"title": {"display": True, "text": "Issue Backlog Over Time (Cumulative Open)"}},
                "scales": {"x": {"ticks": {"maxTicksLimit": 12}}},
            },
        })

    return html


def report_commits(commits):
    html = h2("6. Commits")

    author_counts = Counter(c["author_login"] for c in commits if c["author_login"])
    committer_counts = Counter(c["committer_login"] for c in commits if c["committer_login"])

    by_month = Counter()
    dow_counts = Counter()
    hour_counts = Counter()
    msg_lengths = []
    for c in commits:
        dt = parse_dt(c["date"])
        if dt:
            by_month[month_key(dt)] += 1
            dow_counts[dt.weekday()] += 1
            hour_counts[dt.hour] += 1
        if c["message"]:
            msg_lengths.append(len(c["message"]))

    authors_monthly = {}
    for c in commits:
        user, dt = c["author_login"], parse_dt(c["date"])
        if user and dt:
            authors_monthly.setdefault(month_key(dt), set()).add(user)
    months_a = sorted(authors_monthly)

    total = sum(author_counts.values())
    cumsum, bus_factor = 0, 0
    for _, cnt in author_counts.most_common():
        cumsum += cnt
        bus_factor += 1
        if cumsum >= total * 0.5:
            break

    rows = [
        ("Total commits", f"{len(commits):,}"),
        ("Avg commits per month", f"{np.mean(list(by_month.values())):.1f}" if by_month else "N/A"),
    ]
    if msg_lengths:
        rows.append(("Median commit message length (chars)", f"{np.median(msg_lengths):.0f}"))
    if months_a:
        rows.append(("Avg unique commit authors per month",
                      f"{np.mean([len(authors_monthly[m]) for m in months_a]):.1f}"))
    rows.append(("Bus factor (authors for 50% of commits)", str(bus_factor)))
    html += stats_table(rows)

    top_authors = author_counts.most_common(15)
    html += ranking_table("Top 15 Commit Authors", ["#", "Author", "Commits"],
        [(i, u, f"{c:,}") for i, (u, c) in enumerate(top_authors, 1)])

    top_committers = committer_counts.most_common(10)
    html += ranking_table("Top 10 Committers (merge/push)", ["#", "Committer", "Commits"],
        [(i, u, f"{c:,}") for i, (u, c) in enumerate(top_committers, 1)])

    # Chart: Commits per month
    months = sorted(by_month)
    html += chart({
        "type": "bar",
        "data": {
            "labels": months,
            "datasets": [{"label": "Commits", "data": [by_month[m] for m in months],
                          "backgroundColor": COLORS["blue"]}],
        },
        "options": {"plugins": {"title": {"display": True, "text": "Commits per Month"}}},
    })

    # Chart: Commits by day of week
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    html += chart({
        "type": "bar",
        "data": {
            "labels": days,
            "datasets": [{"label": "Commits", "data": [dow_counts.get(i, 0) for i in range(7)],
                          "backgroundColor": COLORS["green"]}],
        },
        "options": {"plugins": {"title": {"display": True, "text": "Commits by Day of Week"}}},
    })

    # Chart: Commits by hour
    html += chart({
        "type": "bar",
        "data": {
            "labels": [f"{h}:00" for h in range(24)],
            "datasets": [{"label": "Commits", "data": [hour_counts.get(h, 0) for h in range(24)],
                          "backgroundColor": COLORS["orange"]}],
        },
        "options": {"plugins": {"title": {"display": True, "text": "Commits by Hour (UTC)"}}},
    })

    # Chart: Top 15 commit authors (horizontal bar)
    html += chart({
        "type": "bar",
        "data": {
            "labels": [u for u, _ in top_authors][::-1],
            "datasets": [{"label": "Commits", "data": [c for _, c in top_authors][::-1],
                          "backgroundColor": COLORS["green"]}],
        },
        "options": {
            "indexAxis": "y",
            "plugins": {"title": {"display": True, "text": "Top 15 Commit Authors"}},
        },
    })

    # Chart: Unique commit authors per month
    if months_a:
        html += chart({
            "type": "line",
            "data": {
                "labels": months_a,
                "datasets": [{
                    "label": "Unique authors",
                    "data": [len(authors_monthly[m]) for m in months_a],
                    "borderColor": COLORS["blue"],
                    "tension": 0.3,
                }],
            },
            "options": {"plugins": {"title": {"display": True, "text": "Unique Commit Authors per Month"}}},
        })

    return html


def report_ci(runs):
    html = h2("7. CI / Workflow Runs")

    conclusion_counts = Counter(r["conclusion"] for r in runs if r["conclusion"])
    success = conclusion_counts.get("success", 0)
    failure = conclusion_counts.get("failure", 0)
    total_complete = success + failure
    success_rate = success / total_complete * 100 if total_complete else 0

    retries = sum(1 for r in runs if r["run_attempt"] and r["run_attempt"] > 1)

    durations = []
    for r in runs:
        start, end = parse_dt(r["run_started_at"]), parse_dt(r["updated_at"])
        if start and end and r["conclusion"] == "success":
            dur = (end - start).total_seconds() / 60
            if 0 < dur < 600:
                durations.append(dur)

    rows = [("Total workflow runs", f"{len(runs):,}")]
    for conclusion, cnt in conclusion_counts.most_common():
        pct = cnt / len(runs) * 100
        rows.append((f"  {conclusion}", f"{cnt:,} ({pct:.1f}%)"))
    rows += [
        ("CI success rate (success / success+failure)", f"{success_rate:.1f}%"),
        ("Runs with retries (attempt > 1)", f"{retries:,}"),
    ]
    if durations:
        arr = np.array(durations)
        rows += [
            ("Median CI run duration (minutes, success only)", f"{np.median(arr):.1f}"),
            ("90th percentile CI duration (minutes)", f"{np.percentile(arr, 90):.1f}"),
        ]
    html += stats_table(rows)

    event_counts = Counter(r["event"] for r in runs)
    html += ranking_table("Runs by Event Type", ["Event", "Count"],
        [(e, f"{c:,}") for e, c in event_counts.most_common()])

    name_counts = Counter(r["name"] for r in runs)
    html += ranking_table("Runs by Workflow Name", ["Workflow", "Count"],
        [(n, f"{c:,}") for n, c in name_counts.most_common()])

    actor_counts = Counter(r["actor_login"] for r in runs if r["actor_login"])
    html += ranking_table("Top 15 CI Actors", ["#", "Actor", "Runs"],
        [(i, u, f"{c:,}") for i, (u, c) in enumerate(actor_counts.most_common(15), 1)])

    # Chart: CI conclusion breakdown (doughnut)
    html += chart({
        "type": "doughnut",
        "data": {
            "labels": list(conclusion_counts.keys()),
            "datasets": [{"data": list(conclusion_counts.values()),
                          "backgroundColor": [COLORS["green"], COLORS["orange"],
                                              COLORS["red"], COLORS["gray"], COLORS["purple"]]}],
        },
        "options": {"plugins": {"title": {"display": True, "text": "CI Conclusion Breakdown"}}},
    })

    # Chart: CI success rate by month
    monthly_success, monthly_total = {}, {}
    for r in runs:
        dt = parse_dt(r["created_at"])
        if dt and r["conclusion"] in ("success", "failure"):
            mk = month_key(dt)
            monthly_total[mk] = monthly_total.get(mk, 0) + 1
            if r["conclusion"] == "success":
                monthly_success[mk] = monthly_success.get(mk, 0) + 1
    months = sorted(monthly_total)
    if months:
        rates = [round(monthly_success.get(m, 0) / monthly_total[m] * 100, 1) for m in months]
        html += chart({
            "type": "line",
            "data": {
                "labels": months,
                "datasets": [{
                    "label": "Success rate (%)",
                    "data": rates,
                    "borderColor": COLORS["green"],
                    "tension": 0.3,
                }],
            },
            "options": {
                "plugins": {"title": {"display": True, "text": "CI Success Rate by Month"}},
                "scales": {"y": {"min": 0, "max": 100, "title": {"display": True, "text": "%"}}},
            },
        })

    # Chart: CI runs per month
    runs_by_month = Counter()
    for r in runs:
        dt = parse_dt(r["created_at"])
        if dt:
            runs_by_month[month_key(dt)] += 1
    months_r = sorted(runs_by_month)
    html += chart({
        "type": "bar",
        "data": {
            "labels": months_r,
            "datasets": [{"label": "CI runs", "data": [runs_by_month[m] for m in months_r],
                          "backgroundColor": COLORS["orange"]}],
        },
        "options": {"plugins": {"title": {"display": True, "text": "CI Runs per Month"}}},
    })

    # Chart: CI run duration distribution
    if durations:
        labels, counts = histogram_bins(durations, 40)
        html += chart({
            "type": "bar",
            "data": {
                "labels": labels,
                "datasets": [{"label": "Runs", "data": counts, "backgroundColor": COLORS["blue"]}],
            },
            "options": {
                "plugins": {"title": {"display": True, "text": "CI Run Duration Distribution (successful runs)"}},
                "scales": {"x": {"ticks": {"maxTicksLimit": 15},
                                 "title": {"display": True, "text": "Minutes"}}},
            },
        })

    # Chart: Failure rate by workflow name
    name_success, name_fail = Counter(), Counter()
    for r in runs:
        if r["conclusion"] == "success":
            name_success[r["name"]] += 1
        elif r["conclusion"] == "failure":
            name_fail[r["name"]] += 1

    wf_names = [n for n in name_counts if name_counts[n] >= 20]
    if wf_names:
        pairs = []
        for n in wf_names:
            t = name_success.get(n, 0) + name_fail.get(n, 0)
            pairs.append((n, round(name_fail.get(n, 0) / t * 100, 1) if t else 0))
        pairs.sort(key=lambda x: x[1], reverse=True)
        html += chart({
            "type": "bar",
            "data": {
                "labels": [p[0] for p in pairs][::-1],
                "datasets": [{"label": "Failure rate (%)", "data": [p[1] for p in pairs][::-1],
                              "backgroundColor": COLORS["red"]}],
            },
            "options": {
                "indexAxis": "y",
                "plugins": {"title": {"display": True, "text": "CI Failure Rate by Workflow (min 20 runs)"}},
            },
        })

    return html


def report_cross_cutting(prs, issues, commits, runs):
    html = h2("8. Cross-Cutting Analysis")

    pr_monthly, issue_monthly, commit_monthly = Counter(), Counter(), Counter()
    for pr in prs:
        dt = parse_dt(pr["created_at"])
        if dt:
            pr_monthly[month_key(dt)] += 1
    for iss in issues:
        dt = parse_dt(iss["created_at"])
        if dt:
            issue_monthly[month_key(dt)] += 1
    for c in commits:
        dt = parse_dt(c["date"])
        if dt:
            commit_monthly[month_key(dt)] += 1

    activity_by_month = pr_monthly + issue_monthly + commit_monthly
    months = sorted(activity_by_month)

    pr_authors = set(pr["user_login"] for pr in prs if pr["user_login"])
    issue_authors = set(iss["user_login"] for iss in issues if iss["user_login"])
    commit_authors = set(c["author_login"] for c in commits if c["author_login"])
    all_contributors = pr_authors | issue_authors | commit_authors
    all_three = pr_authors & issue_authors & commit_authors

    html += stats_table([
        ("Avg monthly activity (PRs + issues + commits)",
         f"{np.mean(list(activity_by_month.values())):.0f}" if activity_by_month else "N/A"),
        ("Total unique contributors (all types)", str(len(all_contributors))),
        ("Contributors active in all 3 areas", str(len(all_three))),
    ])

    # Chart: Project velocity (stacked bar)
    c_vals = [commit_monthly.get(m, 0) for m in months]
    p_vals = [pr_monthly.get(m, 0) for m in months]
    i_vals = [issue_monthly.get(m, 0) for m in months]
    html += chart({
        "type": "bar",
        "data": {
            "labels": months,
            "datasets": [
                {"label": "Commits", "data": c_vals, "backgroundColor": COLORS["green"]},
                {"label": "PRs", "data": p_vals, "backgroundColor": COLORS["blue"]},
                {"label": "Issues", "data": i_vals, "backgroundColor": COLORS["orange"]},
            ],
        },
        "options": {
            "plugins": {"title": {"display": True, "text": "Overall Project Velocity (Stacked)"}},
            "scales": {"x": {"stacked": True}, "y": {"stacked": True}},
        },
    })

    # Chart: Contributor overlap
    pr_only = pr_authors - issue_authors - commit_authors
    issue_only = issue_authors - pr_authors - commit_authors
    commit_only = commit_authors - pr_authors - issue_authors
    pr_issue = (pr_authors & issue_authors) - commit_authors
    pr_commit = (pr_authors & commit_authors) - issue_authors
    issue_commit = (issue_authors & commit_authors) - pr_authors

    categories = ["PR only", "Issue only", "Commit only",
                   "PR + Issue", "PR + Commit", "Issue + Commit", "All three"]
    sizes = [len(pr_only), len(issue_only), len(commit_only),
             len(pr_issue), len(pr_commit), len(issue_commit), len(all_three)]
    bar_colors = [COLORS["blue"], COLORS["orange"], COLORS["green"],
                  COLORS["purple"], COLORS["brown"], COLORS["pink"], COLORS["red"]]

    html += chart({
        "type": "bar",
        "data": {
            "labels": categories[::-1],
            "datasets": [{"label": "Contributors", "data": sizes[::-1],
                          "backgroundColor": bar_colors[::-1]}],
        },
        "options": {
            "indexAxis": "y",
            "plugins": {"title": {"display": True, "text": "Contributor Activity Overlap"}},
        },
    })

    # Heatmap: Activity by day of week and hour
    dow_hour = [[0]*24 for _ in range(7)]
    for pr in prs:
        dt = parse_dt(pr["created_at"])
        if dt:
            dow_hour[dt.weekday()][dt.hour] += 1
    for c in commits:
        dt = parse_dt(c["date"])
        if dt:
            dow_hour[dt.weekday()][dt.hour] += 1

    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    html += heatmap_table(dow_hour, days, list(range(24)),
                          "Activity Heatmap: Day of Week vs Hour (PRs + Commits)")

    return html


HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bitcoin Core GitHub Statistics Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    max-width: 1200px;
    margin: 0 auto;
    padding: 2rem;
    background: #fafafa;
    color: #1a1a1a;
  }}
  h1 {{
    border-bottom: 3px solid #f7931a;
    padding-bottom: 0.5rem;
    color: #1a1a1a;
  }}
  h1 small {{
    font-weight: normal;
    color: #666;
    font-size: 0.5em;
    display: block;
    margin-top: 0.3rem;
  }}
  h2 {{
    margin-top: 3rem;
    padding-top: 1.5rem;
    border-top: 1px solid #ddd;
    color: #333;
  }}
  h3 {{ color: #555; }}
  .chart-wrap {{
    max-width: 100%;
    margin: 1.5rem 0;
    background: #fff;
    border-radius: 4px;
    padding: 1rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.1);
  }}
  table {{
    border-collapse: collapse;
    margin: 1rem 0;
  }}
  table.stats td {{
    padding: 0.35rem 1rem 0.35rem 0;
    border-bottom: 1px solid #eee;
  }}
  table.stats td:last-child {{
    font-family: "SF Mono", "Menlo", monospace;
    text-align: right;
  }}
  table.ranking {{
    width: auto;
  }}
  table.ranking th {{
    text-align: left;
    padding: 0.4rem 1.2rem 0.4rem 0;
    border-bottom: 2px solid #ddd;
    color: #555;
    font-size: 0.9em;
  }}
  table.ranking td {{
    padding: 0.3rem 1.2rem 0.3rem 0;
    border-bottom: 1px solid #eee;
    font-family: "SF Mono", "Menlo", monospace;
    font-size: 0.9em;
  }}
  table.heatmap {{
    border-collapse: collapse;
    margin: 1.5rem 0;
    font-size: 0.75em;
  }}
  table.heatmap th {{
    padding: 0.3rem 0.4rem;
    font-weight: normal;
    color: #555;
  }}
  table.heatmap .hm-label {{
    text-align: right;
    padding-right: 0.6rem;
    font-weight: 600;
    color: #333;
  }}
  table.heatmap .hm-cell {{
    text-align: center;
    padding: 0.3rem 0.4rem;
    min-width: 2rem;
    border-radius: 2px;
    cursor: default;
  }}
</style>
</head>
<body>
<h1>Bitcoin Core — GitHub Statistics Report
  <small>Generated {generated} · ~12 months of activity</small>
</h1>
{body}
</body>
</html>
"""


def main():
    conn = connect()
    prs = load_prs(conn)
    issues = load_issues(conn)
    commits = load_commits(conn)
    runs = load_runs(conn)

    body = ""
    body += report_overview(conn)
    body += report_pr_activity(prs)
    body += report_pr_authors(prs)
    body += report_pr_timing(prs)
    body += report_issues(issues)
    body += report_commits(commits)
    body += report_ci(runs)
    body += report_cross_cutting(prs, issues, commits, runs)

    conn.close()

    html = HTML_TEMPLATE.format(
        generated=datetime.now().strftime("%Y-%m-%d %H:%M"),
        body=body,
    )

    with open(OUT_FILE, "w") as f:
        f.write(html)

    print(f"Report written to {OUT_FILE}")


if __name__ == "__main__":
    main()
