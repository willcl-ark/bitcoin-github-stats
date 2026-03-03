#!/usr/bin/env python3
"""
Bitcoin Core GitHub Statistics Report
Analyzes 12 months of scraped data from bitcoin/bitcoin.
Outputs a single self-contained HTML file.
"""

import base64
import io
import sqlite3
import json
from datetime import datetime, timedelta
from collections import Counter
from html import escape

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

DB_PATH = "gh-stats.db"
OUT_FILE = "report.html"
CUTOFF = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")

plt.rcParams.update({
    "figure.figsize": (14, 6),
    "figure.dpi": 150,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
})

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


def fig_to_img_tag(fig):
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("ascii")
    return f'<img src="data:image/png;base64,{b64}">'


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
# Report sections — each returns an HTML string
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
    fig, ax = plt.subplots()
    x = range(len(months))
    w = 0.4
    ax.bar([i - w/2 for i in x], [opened_by_month[m] for m in months], w,
           label="Opened", color=COLORS["blue"])
    ax.bar([i + w/2 for i in x], [merged_by_month[m] for m in months], w,
           label="Merged", color=COLORS["green"])
    ax.set_xticks(list(x))
    ax.set_xticklabels(months, rotation=45, ha="right")
    ax.set_title("Pull Requests Opened vs Merged per Month")
    ax.set_ylabel("Count")
    ax.legend()
    html += fig_to_img_tag(fig)

    # Chart: Time to merge distribution
    if merge_days:
        fig, ax = plt.subplots()
        capped = [min(d, 365) for d in merge_days]
        ax.hist(capped, bins=50, color=COLORS["blue"], edgecolor="white", alpha=0.8)
        ax.axvline(np.median(merge_days), color=COLORS["red"], linestyle="--",
                    label=f"Median: {np.median(merge_days):.0f}d")
        ax.set_title("Time to Merge Distribution (capped at 365 days)")
        ax.set_xlabel("Days")
        ax.set_ylabel("PR Count")
        ax.legend()
        html += fig_to_img_tag(fig)

    # Chart: PR state breakdown
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie([len(merged), len(closed_no_merge), len(open_prs)],
           labels=["Merged", "Closed (no merge)", "Open"],
           colors=[COLORS["green"], COLORS["red"], COLORS["blue"]],
           autopct="%1.1f%%", startangle=90)
    ax.set_title("Pull Request State Breakdown")
    html += fig_to_img_tag(fig)

    # Chart: Median merge time trend
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
        fig, ax = plt.subplots()
        medians = [np.median(monthly_merge_times[m]) for m in months_mt]
        p25 = [np.percentile(monthly_merge_times[m], 25) for m in months_mt]
        p75 = [np.percentile(monthly_merge_times[m], 75) for m in months_mt]
        ax.fill_between(range(len(months_mt)), p25, p75, alpha=0.2, color=COLORS["blue"])
        ax.plot(medians, color=COLORS["blue"], marker="o", markersize=4)
        ax.set_xticks(range(len(months_mt)))
        ax.set_xticklabels(months_mt, rotation=45, ha="right")
        ax.set_title("Median Time to Merge Trend (with IQR)")
        ax.set_ylabel("Days")
        html += fig_to_img_tag(fig)

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

    # New PR authors per month
    first_pr = {}
    for pr in prs:
        user, dt = pr["user_login"], parse_dt(pr["created_at"])
        if user and dt:
            mk = month_key(dt)
            if user not in first_pr or mk < first_pr[user]:
                first_pr[user] = mk
    new_per_month = Counter(first_pr.values())

    # Unique authors per month
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

    # Chart: Top 20 PR authors
    fig, ax = plt.subplots()
    users = [u for u, _ in top20]
    counts = [c for _, c in top20]
    ax.barh(users[::-1], counts[::-1], color=COLORS["blue"])
    ax.set_title("Top 20 PR Authors")
    ax.set_xlabel("Pull Requests")
    html += fig_to_img_tag(fig)

    # Chart: Author diversity
    if months_a:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        ax1.bar(range(len(months_a)),
                [len(authors_per_month[m]) for m in months_a],
                color=COLORS["blue"])
        ax1.set_xticks(range(len(months_a)))
        ax1.set_xticklabels(months_a, rotation=45, ha="right")
        ax1.set_title("Unique PR Authors per Month")
        ax1.set_ylabel("Authors")

        nm = sorted(new_per_month)
        ax2.bar(range(len(nm)), [new_per_month[m] for m in nm], color=COLORS["green"])
        ax2.set_xticks(range(len(nm)))
        ax2.set_xticklabels(nm, rotation=45, ha="right")
        ax2.set_title("New (First-time) PR Authors per Month")
        ax2.set_ylabel("New Authors")
        html += fig_to_img_tag(fig)

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

    html += ranking_table("PRs Opened by Day of Week", ["Day", "Count"],
        [(day, f"{dow_counts.get(i, 0):,}") for i, day in enumerate(days)])
    html += stats_table([
        ("Weekend vs weekday PR ratio", f"{weekend/(weekday or 1)*100:.1f}% of weekday volume"),
        ("Peak hour for PR creation (UTC)", f"{peak_hour}:00"),
    ])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    ax1.bar(days, [dow_counts.get(i, 0) for i in range(7)], color=COLORS["blue"])
    ax1.set_title("PRs Opened by Day of Week")
    ax1.set_ylabel("Count")

    hours = list(range(24))
    ax2.bar(hours, [hour_counts.get(h, 0) for h in hours], color=COLORS["orange"])
    ax2.set_title("PRs Opened by Hour (UTC)")
    ax2.set_xlabel("Hour (UTC)")
    ax2.set_ylabel("Count")
    ax2.set_xticks(hours[::2])
    html += fig_to_img_tag(fig)

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

    # Labels
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
    fig, ax = plt.subplots()
    x = range(len(months))
    w = 0.4
    ax.bar([i - w/2 for i in x], [opened_by_month[m] for m in months], w,
           label="Opened", color=COLORS["blue"])
    ax.bar([i + w/2 for i in x], [closed_by_month[m] for m in months], w,
           label="Closed", color=COLORS["green"])
    ax.set_xticks(list(x))
    ax.set_xticklabels(months, rotation=45, ha="right")
    ax.set_title("Issues Opened vs Closed per Month")
    ax.set_ylabel("Count")
    ax.legend()
    html += fig_to_img_tag(fig)

    # Chart: Issue labels
    if top_labels:
        fig, ax = plt.subplots(figsize=(14, 8))
        ax.barh([l for l, _ in top_labels][::-1], [c for _, c in top_labels][::-1],
                color=COLORS["purple"])
        ax.set_title("Top 20 Issue Labels")
        ax.set_xlabel("Count")
        html += fig_to_img_tag(fig)

    # Chart: Issue close time distribution
    if close_days:
        fig, ax = plt.subplots()
        capped = [min(d, 365) for d in close_days]
        ax.hist(capped, bins=50, color=COLORS["green"], edgecolor="white", alpha=0.8)
        ax.axvline(np.median(close_days), color=COLORS["red"], linestyle="--",
                    label=f"Median: {np.median(close_days):.0f}d")
        ax.set_title("Time to Close Issues (capped at 365 days)")
        ax.set_xlabel("Days")
        ax.set_ylabel("Count")
        ax.legend()
        html += fig_to_img_tag(fig)

    # Chart: Issue backlog
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
            dates.append(dt)
            cumulative.append(running)

        fig, ax = plt.subplots()
        ax.plot(dates, cumulative, color=COLORS["blue"], linewidth=0.8)
        ax.set_title("Issue Backlog Over Time (Cumulative Open Issues)")
        ax.set_ylabel("Open Issues")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.xticks(rotation=45)
        html += fig_to_img_tag(fig)

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
    fig, ax = plt.subplots()
    ax.bar(range(len(months)), [by_month[m] for m in months], color=COLORS["blue"])
    ax.set_xticks(range(len(months)))
    ax.set_xticklabels(months, rotation=45, ha="right")
    ax.set_title("Commits per Month")
    ax.set_ylabel("Commits")
    html += fig_to_img_tag(fig)

    # Chart: Commits by day of week and hour
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    ax1.bar(days, [dow_counts.get(i, 0) for i in range(7)], color=COLORS["green"])
    ax1.set_title("Commits by Day of Week")
    ax1.set_ylabel("Count")
    hours = list(range(24))
    ax2.bar(hours, [hour_counts.get(h, 0) for h in hours], color=COLORS["orange"])
    ax2.set_title("Commits by Hour (UTC)")
    ax2.set_xlabel("Hour (UTC)")
    ax2.set_ylabel("Count")
    ax2.set_xticks(hours[::2])
    html += fig_to_img_tag(fig)

    # Chart: Top 15 commit authors
    fig, ax = plt.subplots()
    ax.barh([u for u, _ in top_authors][::-1], [c for _, c in top_authors][::-1],
            color=COLORS["green"])
    ax.set_title("Top 15 Commit Authors")
    ax.set_xlabel("Commits")
    html += fig_to_img_tag(fig)

    # Chart: Unique authors per month
    if months_a:
        fig, ax = plt.subplots()
        ax.plot(months_a, [len(authors_monthly[m]) for m in months_a],
                marker="o", color=COLORS["blue"])
        ax.set_title("Unique Commit Authors per Month")
        ax.set_ylabel("Authors")
        plt.xticks(rotation=45, ha="right")
        html += fig_to_img_tag(fig)

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

    # Chart: CI conclusion breakdown
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie(list(conclusion_counts.values()), labels=list(conclusion_counts.keys()),
           autopct="%1.1f%%", startangle=90)
    ax.set_title("CI Conclusion Breakdown")
    html += fig_to_img_tag(fig)

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
        fig, ax = plt.subplots()
        rates = [monthly_success.get(m, 0) / monthly_total[m] * 100 for m in months]
        ax.plot(months, rates, marker="o", color=COLORS["green"], linewidth=2)
        ax.set_title("CI Success Rate by Month")
        ax.set_ylabel("Success Rate (%)")
        ax.set_ylim(0, 100)
        plt.xticks(rotation=45, ha="right")
        html += fig_to_img_tag(fig)

    # Chart: CI runs per month
    runs_by_month = Counter()
    for r in runs:
        dt = parse_dt(r["created_at"])
        if dt:
            runs_by_month[month_key(dt)] += 1
    months_r = sorted(runs_by_month)
    fig, ax = plt.subplots()
    ax.bar(range(len(months_r)), [runs_by_month[m] for m in months_r], color=COLORS["orange"])
    ax.set_xticks(range(len(months_r)))
    ax.set_xticklabels(months_r, rotation=45, ha="right")
    ax.set_title("CI Runs per Month")
    ax.set_ylabel("Runs")
    html += fig_to_img_tag(fig)

    # Chart: CI run duration distribution
    if durations:
        fig, ax = plt.subplots()
        ax.hist(durations, bins=50, color=COLORS["blue"], edgecolor="white", alpha=0.8)
        ax.axvline(np.median(durations), color=COLORS["red"], linestyle="--",
                    label=f"Median: {np.median(durations):.0f}m")
        ax.set_title("CI Run Duration Distribution (successful runs)")
        ax.set_xlabel("Minutes")
        ax.set_ylabel("Count")
        ax.legend()
        html += fig_to_img_tag(fig)

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
            total = name_success.get(n, 0) + name_fail.get(n, 0)
            pairs.append((n, name_fail.get(n, 0) / total * 100 if total else 0))
        pairs.sort(key=lambda x: x[1], reverse=True)
        fig, ax = plt.subplots()
        ax.barh([p[0] for p in pairs][::-1], [p[1] for p in pairs][::-1], color=COLORS["red"])
        ax.set_title("CI Failure Rate by Workflow (min 20 runs)")
        ax.set_xlabel("Failure Rate (%)")
        html += fig_to_img_tag(fig)

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

    # Chart: Project velocity
    fig, ax = plt.subplots()
    x = range(len(months))
    c_vals = [commit_monthly.get(m, 0) for m in months]
    p_vals = [pr_monthly.get(m, 0) for m in months]
    i_vals = [issue_monthly.get(m, 0) for m in months]
    ax.bar(x, c_vals, label="Commits", color=COLORS["green"])
    ax.bar(x, p_vals, bottom=c_vals, label="PRs", color=COLORS["blue"])
    ax.bar(x, i_vals, bottom=[c + p for c, p in zip(c_vals, p_vals)],
           label="Issues", color=COLORS["orange"])
    ax.set_xticks(list(x))
    ax.set_xticklabels(months, rotation=45, ha="right")
    ax.set_title("Overall Project Velocity (Stacked)")
    ax.set_ylabel("Activity Count")
    ax.legend()
    html += fig_to_img_tag(fig)

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

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(categories[::-1], sizes[::-1],
                   color=[COLORS["blue"], COLORS["green"], COLORS["orange"],
                          COLORS["purple"], COLORS["brown"], COLORS["pink"],
                          COLORS["red"]][::-1])
    ax.set_title("Contributor Activity Overlap")
    ax.set_xlabel("Contributors")
    for bar, val in zip(bars, sizes[::-1]):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                str(val), va="center")
    html += fig_to_img_tag(fig)

    # Chart: Activity heatmap
    dow_hour = np.zeros((7, 24))
    for pr in prs:
        dt = parse_dt(pr["created_at"])
        if dt:
            dow_hour[dt.weekday()][dt.hour] += 1
    for c in commits:
        dt = parse_dt(c["date"])
        if dt:
            dow_hour[dt.weekday()][dt.hour] += 1

    fig, ax = plt.subplots(figsize=(16, 5))
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    im = ax.imshow(dow_hour, aspect="auto", cmap="YlOrRd")
    ax.set_yticks(range(7))
    ax.set_yticklabels(days)
    ax.set_xticks(range(24))
    ax.set_xticklabels([str(h) for h in range(24)])
    ax.set_xlabel("Hour (UTC)")
    ax.set_title("Activity Heatmap: Day of Week vs Hour (PRs + Commits)")
    fig.colorbar(im, ax=ax, label="Activity Count")
    html += fig_to_img_tag(fig)

    return html


HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bitcoin Core GitHub Statistics Report</title>
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
  img {{
    max-width: 100%;
    height: auto;
    display: block;
    margin: 1.5rem 0;
    border-radius: 4px;
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
