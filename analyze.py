#!/usr/bin/env python3
"""
Bitcoin Core GitHub Statistics Report
Analyzes 12 months of scraped data from bitcoin/bitcoin.
"""

import sqlite3
import json
from datetime import datetime, timedelta
from collections import Counter
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import numpy as np

DB_PATH = "gh-stats.db"
OUT_DIR = Path("report")
OUT_DIR.mkdir(exist_ok=True)

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


def save(fig, name):
    fig.tight_layout()
    fig.savefig(OUT_DIR / f"{name}.png")
    plt.close(fig)


# ──────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────

def load_prs(conn):
    return conn.execute("SELECT * FROM pull_requests").fetchall()

def load_issues(conn):
    return conn.execute("SELECT * FROM issues WHERE is_pull_request = 0").fetchall()

def load_commits(conn):
    return conn.execute("SELECT * FROM commits ORDER BY date").fetchall()

def load_runs(conn):
    return conn.execute("SELECT * FROM workflow_runs").fetchall()


# ──────────────────────────────────────────────
# Report sections
# ──────────────────────────────────────────────

def section_header(f, title, n):
    f.write(f"\n{'='*70}\n")
    f.write(f"  SECTION {n}: {title}\n")
    f.write(f"{'='*70}\n\n")


def stat(f, num, label, value):
    f.write(f"  [{num:>2}] {label:<55} {value}\n")


def report_overview(f, conn):
    section_header(f, "OVERVIEW", 1)
    counts = {}
    for tbl in ("workflow_runs", "pull_requests", "issues", "commits"):
        row = conn.execute(f"SELECT COUNT(*) as c FROM {tbl}").fetchone()
        counts[tbl] = row["c"]

    issue_count = conn.execute(
        "SELECT COUNT(*) as c FROM issues WHERE is_pull_request = 0"
    ).fetchone()["c"]

    stat(f, 1, "Total workflow runs", f"{counts['workflow_runs']:,}")
    stat(f, 2, "Total pull requests", f"{counts['pull_requests']:,}")
    stat(f, 3, "Total issues (excluding PRs)", f"{issue_count:,}")
    stat(f, 4, "Total commits", f"{counts['commits']:,}")

    pr_range = conn.execute(
        "SELECT MIN(created_at), MAX(created_at) FROM pull_requests"
    ).fetchone()
    stat(f, 5, "Data range (PRs)",
         f"{pr_range[0][:10]} to {pr_range[1][:10]}")

    unique_pr_authors = conn.execute(
        "SELECT COUNT(DISTINCT user_login) FROM pull_requests"
    ).fetchone()[0]
    unique_committers = conn.execute(
        "SELECT COUNT(DISTINCT author_login) FROM commits WHERE author_login IS NOT NULL"
    ).fetchone()[0]
    stat(f, 6, "Unique PR authors", str(unique_pr_authors))
    stat(f, 7, "Unique commit authors", str(unique_committers))


def report_pr_activity(f, conn, prs):
    section_header(f, "PULL REQUEST ACTIVITY", 2)

    merged = [r for r in prs if r["merged_at"]]
    closed_no_merge = [r for r in prs if r["state"] == "closed" and not r["merged_at"]]
    open_prs = [r for r in prs if r["state"] == "open"]

    stat(f, 8, "PRs merged", f"{len(merged):,}")
    stat(f, 9, "PRs closed without merge", f"{len(closed_no_merge):,}")
    stat(f, 10, "PRs currently open", f"{len(open_prs):,}")
    total_closed = len(merged) + len(closed_no_merge)
    merge_rate = len(merged) / total_closed * 100 if total_closed else 0
    stat(f, 11, "Merge rate (merged / all closed)", f"{merge_rate:.1f}%")

    # Time to merge
    merge_days = []
    for pr in merged:
        created = parse_dt(pr["created_at"])
        m = parse_dt(pr["merged_at"])
        if created and m:
            merge_days.append((m - created).total_seconds() / 86400)

    if merge_days:
        arr = np.array(merge_days)
        stat(f, 12, "Median time to merge (days)", f"{np.median(arr):.1f}")
        stat(f, 13, "Mean time to merge (days)", f"{np.mean(arr):.1f}")
        stat(f, 14, "90th percentile time to merge (days)", f"{np.percentile(arr, 90):.1f}")
        stat(f, 15, "Fastest merge (hours)", f"{np.min(arr)*24:.1f}")

    # Time to close (unmerged)
    close_days = []
    for pr in closed_no_merge:
        created = parse_dt(pr["created_at"])
        closed = parse_dt(pr["closed_at"])
        if created and closed:
            close_days.append((closed - created).total_seconds() / 86400)

    if close_days:
        arr = np.array(close_days)
        stat(f, 16, "Median time to close without merge (days)", f"{np.median(arr):.1f}")

    # Draft PRs
    drafts = sum(1 for pr in prs if pr["draft"])
    stat(f, 17, "Draft PRs", f"{drafts:,}")

    # ── Chart: PRs opened/merged per month ──
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
    save(fig, "08_prs_opened_merged_per_month")

    # ── Chart: Time to merge distribution ──
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
        save(fig, "09_time_to_merge_distribution")

    # ── Chart: PR state breakdown ──
    fig, ax = plt.subplots(figsize=(8, 8))
    labels = ["Merged", "Closed (no merge)", "Open"]
    sizes = [len(merged), len(closed_no_merge), len(open_prs)]
    colors = [COLORS["green"], COLORS["red"], COLORS["blue"]]
    ax.pie(sizes, labels=labels, colors=colors, autopct="%1.1f%%", startangle=90)
    ax.set_title("Pull Request State Breakdown")
    save(fig, "10_pr_state_breakdown")

    # ── Chart: Median merge time trend ──
    monthly_merge_times = {}
    for pr in merged:
        created = parse_dt(pr["created_at"])
        m = parse_dt(pr["merged_at"])
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
        save(fig, "11_merge_time_trend")


def report_pr_authors(f, conn, prs):
    section_header(f, "PULL REQUEST AUTHORS", 3)

    author_counts = Counter(pr["user_login"] for pr in prs if pr["user_login"])
    top20 = author_counts.most_common(20)

    f.write("  [18] Top 20 PR authors:\n")
    for i, (user, cnt) in enumerate(top20, 1):
        f.write(f"        {i:>2}. {user:<30} {cnt:>4} PRs\n")

    # Authors with merged PRs
    merged_authors = Counter(
        pr["user_login"] for pr in prs if pr["merged_at"] and pr["user_login"]
    )
    top_merged = merged_authors.most_common(10)
    f.write("\n  [19] Top 10 authors by merged PRs:\n")
    for i, (user, cnt) in enumerate(top_merged, 1):
        f.write(f"        {i:>2}. {user:<30} {cnt:>4} merged\n")

    # New PR authors per month
    first_pr = {}
    for pr in prs:
        user = pr["user_login"]
        dt = parse_dt(pr["created_at"])
        if user and dt:
            mk = month_key(dt)
            if user not in first_pr or mk < first_pr[user]:
                first_pr[user] = mk

    new_per_month = Counter(first_pr.values())
    months = sorted(new_per_month)
    stat(f, 20, "New PR authors per month (avg)",
         f"{np.mean(list(new_per_month.values())):.1f}" if new_per_month else "N/A")

    # Unique authors per month
    authors_per_month = {}
    for pr in prs:
        user = pr["user_login"]
        dt = parse_dt(pr["created_at"])
        if user and dt:
            mk = month_key(dt)
            authors_per_month.setdefault(mk, set()).add(user)

    months_a = sorted(authors_per_month)
    if months_a:
        stat(f, 21, "Avg unique PR authors per month",
             f"{np.mean([len(authors_per_month[m]) for m in months_a]):.1f}")

    # ── Chart: Top 20 PR authors ──
    fig, ax = plt.subplots()
    users = [u for u, _ in top20]
    counts = [c for _, c in top20]
    ax.barh(users[::-1], counts[::-1], color=COLORS["blue"])
    ax.set_title("Top 20 PR Authors")
    ax.set_xlabel("Pull Requests")
    save(fig, "12_top_pr_authors")

    # ── Chart: Unique authors per month ──
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
        save(fig, "13_pr_author_diversity")


def report_pr_timing(f, prs):
    section_header(f, "PULL REQUEST TIMING PATTERNS", 4)

    dow_counts = Counter()
    hour_counts = Counter()
    for pr in prs:
        dt = parse_dt(pr["created_at"])
        if dt:
            dow_counts[dt.weekday()] += 1
            hour_counts[dt.hour] += 1

    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    f.write("  [22] PRs opened by day of week:\n")
    for i, day in enumerate(days):
        f.write(f"        {day}: {dow_counts.get(i, 0):>4}\n")

    weekend = dow_counts.get(5, 0) + dow_counts.get(6, 0)
    weekday = sum(dow_counts.get(i, 0) for i in range(5))
    stat(f, 23, "Weekend vs weekday PR ratio",
         f"{weekend/(weekday or 1)*100:.1f}% of weekday volume")

    peak_hour = max(hour_counts, key=hour_counts.get) if hour_counts else 0
    stat(f, 24, "Peak hour for PR creation (UTC)", f"{peak_hour}:00")

    # ── Chart: PRs by day of week and hour ──
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
    save(fig, "14_pr_timing_patterns")


def report_issues(f, conn, issues):
    section_header(f, "ISSUES", 5)

    open_issues = [i for i in issues if i["state"] == "open"]
    closed_issues = [i for i in issues if i["state"] == "closed"]
    stat(f, 25, "Total issues (non-PR)", f"{len(issues):,}")
    stat(f, 26, "Open issues", f"{len(open_issues):,}")
    stat(f, 27, "Closed issues", f"{len(closed_issues):,}")

    # Time to close
    close_days = []
    for iss in closed_issues:
        created = parse_dt(iss["created_at"])
        closed = parse_dt(iss["closed_at"])
        if created and closed:
            close_days.append((closed - created).total_seconds() / 86400)

    if close_days:
        arr = np.array(close_days)
        stat(f, 28, "Median time to close issues (days)", f"{np.median(arr):.1f}")
        stat(f, 29, "Mean time to close issues (days)", f"{np.mean(arr):.1f}")

    # Labels
    all_labels = []
    for iss in issues:
        raw = iss["labels"]
        if raw:
            try:
                labels = json.loads(raw)
                all_labels.extend(labels)
            except (json.JSONDecodeError, TypeError):
                pass

    label_counts = Counter(all_labels)
    top_labels = label_counts.most_common(20)
    f.write("\n  [30] Top 20 issue labels:\n")
    for i, (label, cnt) in enumerate(top_labels, 1):
        f.write(f"        {i:>2}. {label:<40} {cnt:>4}\n")

    # Top issue reporters
    reporter_counts = Counter(iss["user_login"] for iss in issues if iss["user_login"])
    top_reporters = reporter_counts.most_common(15)
    f.write("\n  [31] Top 15 issue reporters:\n")
    for i, (user, cnt) in enumerate(top_reporters, 1):
        f.write(f"        {i:>2}. {user:<30} {cnt:>4} issues\n")

    # Issues opened per month
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

    # ── Chart: Issues opened/closed per month ──
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
    save(fig, "15_issues_per_month")

    # ── Chart: Issue label distribution ──
    if top_labels:
        fig, ax = plt.subplots(figsize=(14, 8))
        labels_l = [l for l, _ in top_labels]
        counts_l = [c for _, c in top_labels]
        ax.barh(labels_l[::-1], counts_l[::-1], color=COLORS["purple"])
        ax.set_title("Top 20 Issue Labels")
        ax.set_xlabel("Count")
        save(fig, "16_issue_labels")

    # ── Chart: Issue close time distribution ──
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
        save(fig, "17_issue_close_time")

    # ── Chart: Issue backlog (cumulative open) ──
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
        cumulative = []
        running = 0
        dates = []
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
        save(fig, "18_issue_backlog")


def report_commits(f, conn, commits):
    section_header(f, "COMMITS", 6)

    stat(f, 32, "Total commits", f"{len(commits):,}")

    author_counts = Counter(c["author_login"] for c in commits if c["author_login"])
    committer_counts = Counter(c["committer_login"] for c in commits if c["committer_login"])

    top_authors = author_counts.most_common(15)
    f.write("\n  [33] Top 15 commit authors:\n")
    for i, (user, cnt) in enumerate(top_authors, 1):
        f.write(f"        {i:>2}. {user:<30} {cnt:>4} commits\n")

    top_committers = committer_counts.most_common(10)
    f.write("\n  [34] Top 10 committers (merge/push):\n")
    for i, (user, cnt) in enumerate(top_committers, 1):
        f.write(f"        {i:>2}. {user:<30} {cnt:>4} commits\n")

    # Commits per month
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

    months = sorted(by_month)
    stat(f, 35, "Avg commits per month",
         f"{np.mean(list(by_month.values())):.1f}" if by_month else "N/A")

    if msg_lengths:
        stat(f, 36, "Median commit message length (chars)", f"{np.median(msg_lengths):.0f}")

    # Unique authors per month
    authors_monthly = {}
    for c in commits:
        user = c["author_login"]
        dt = parse_dt(c["date"])
        if user and dt:
            mk = month_key(dt)
            authors_monthly.setdefault(mk, set()).add(user)

    months_a = sorted(authors_monthly)
    if months_a:
        stat(f, 37, "Avg unique commit authors per month",
             f"{np.mean([len(authors_monthly[m]) for m in months_a]):.1f}")

    # Bus factor: how many authors make up 50% of commits
    total = sum(author_counts.values())
    cumsum = 0
    bus_factor = 0
    for _, cnt in author_counts.most_common():
        cumsum += cnt
        bus_factor += 1
        if cumsum >= total * 0.5:
            break
    stat(f, 38, "Bus factor (authors for 50% of commits)", str(bus_factor))

    # ── Chart: Commits per month ──
    fig, ax = plt.subplots()
    ax.bar(range(len(months)), [by_month[m] for m in months], color=COLORS["blue"])
    ax.set_xticks(range(len(months)))
    ax.set_xticklabels(months, rotation=45, ha="right")
    ax.set_title("Commits per Month")
    ax.set_ylabel("Commits")
    save(fig, "19_commits_per_month")

    # ── Chart: Commits by day of week and hour ──
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
    save(fig, "20_commit_timing")

    # ── Chart: Top 15 commit authors ──
    fig, ax = plt.subplots()
    users = [u for u, _ in top_authors]
    cnts = [c for _, c in top_authors]
    ax.barh(users[::-1], cnts[::-1], color=COLORS["green"])
    ax.set_title("Top 15 Commit Authors")
    ax.set_xlabel("Commits")
    save(fig, "21_top_commit_authors")

    # ── Chart: Unique commit authors per month ──
    if months_a:
        fig, ax = plt.subplots()
        ax.plot(months_a, [len(authors_monthly[m]) for m in months_a],
                marker="o", color=COLORS["blue"])
        ax.set_title("Unique Commit Authors per Month")
        ax.set_ylabel("Authors")
        plt.xticks(rotation=45, ha="right")
        save(fig, "22_commit_author_diversity")


def report_ci(f, conn, runs):
    section_header(f, "CI / WORKFLOW RUNS", 7)

    stat(f, 39, "Total workflow runs", f"{len(runs):,}")

    conclusion_counts = Counter(r["conclusion"] for r in runs if r["conclusion"])
    f.write("\n  [40] Conclusion breakdown:\n")
    for conclusion, cnt in conclusion_counts.most_common():
        pct = cnt / len(runs) * 100
        f.write(f"        {conclusion:<25} {cnt:>5} ({pct:.1f}%)\n")

    success = conclusion_counts.get("success", 0)
    failure = conclusion_counts.get("failure", 0)
    total_complete = success + failure
    success_rate = success / total_complete * 100 if total_complete else 0
    stat(f, 41, "CI success rate (success / success+failure)", f"{success_rate:.1f}%")

    # Event types
    event_counts = Counter(r["event"] for r in runs)
    f.write("\n  [42] Runs by event type:\n")
    for event, cnt in event_counts.most_common():
        f.write(f"        {event:<25} {cnt:>5}\n")

    # Workflow names
    name_counts = Counter(r["name"] for r in runs)
    f.write("\n  [43] Runs by workflow name:\n")
    for name, cnt in name_counts.most_common():
        f.write(f"        {name:<45} {cnt:>5}\n")

    # Top CI actors
    actor_counts = Counter(r["actor_login"] for r in runs if r["actor_login"])
    top_actors = actor_counts.most_common(15)
    f.write("\n  [44] Top 15 CI actors:\n")
    for i, (user, cnt) in enumerate(top_actors, 1):
        f.write(f"        {i:>2}. {user:<30} {cnt:>4} runs\n")

    # Run retries
    retries = sum(1 for r in runs if r["run_attempt"] and r["run_attempt"] > 1)
    stat(f, 45, "Runs with retries (attempt > 1)", f"{retries:,}")

    # CI success rate by month
    monthly_success = {}
    monthly_total = {}
    for r in runs:
        dt = parse_dt(r["created_at"])
        if dt and r["conclusion"] in ("success", "failure"):
            mk = month_key(dt)
            monthly_total[mk] = monthly_total.get(mk, 0) + 1
            if r["conclusion"] == "success":
                monthly_success[mk] = monthly_success.get(mk, 0) + 1

    months = sorted(monthly_total)

    # CI run duration
    durations = []
    for r in runs:
        start = parse_dt(r["run_started_at"])
        end = parse_dt(r["updated_at"])
        if start and end and r["conclusion"] == "success":
            dur = (end - start).total_seconds() / 60
            if 0 < dur < 600:
                durations.append(dur)

    if durations:
        arr = np.array(durations)
        stat(f, 46, "Median CI run duration (minutes, success only)", f"{np.median(arr):.1f}")
        stat(f, 47, "90th percentile CI duration (minutes)", f"{np.percentile(arr, 90):.1f}")

    # ── Chart: CI conclusion breakdown ──
    fig, ax = plt.subplots(figsize=(8, 8))
    labels = list(conclusion_counts.keys())
    sizes = list(conclusion_counts.values())
    ax.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=90)
    ax.set_title("CI Conclusion Breakdown")
    save(fig, "23_ci_conclusion_breakdown")

    # ── Chart: CI success rate by month ──
    if months:
        fig, ax = plt.subplots()
        rates = [monthly_success.get(m, 0) / monthly_total[m] * 100 for m in months]
        ax.plot(months, rates, marker="o", color=COLORS["green"], linewidth=2)
        ax.set_title("CI Success Rate by Month")
        ax.set_ylabel("Success Rate (%)")
        ax.set_ylim(0, 100)
        plt.xticks(rotation=45, ha="right")
        save(fig, "24_ci_success_rate_trend")

    # ── Chart: CI runs per month ──
    runs_by_month = Counter()
    for r in runs:
        dt = parse_dt(r["created_at"])
        if dt:
            runs_by_month[month_key(dt)] += 1
    months_r = sorted(runs_by_month)
    fig, ax = plt.subplots()
    ax.bar(range(len(months_r)), [runs_by_month[m] for m in months_r],
           color=COLORS["orange"])
    ax.set_xticks(range(len(months_r)))
    ax.set_xticklabels(months_r, rotation=45, ha="right")
    ax.set_title("CI Runs per Month")
    ax.set_ylabel("Runs")
    save(fig, "25_ci_runs_per_month")

    # ── Chart: CI run duration distribution ──
    if durations:
        fig, ax = plt.subplots()
        ax.hist(durations, bins=50, color=COLORS["blue"], edgecolor="white", alpha=0.8)
        ax.axvline(np.median(durations), color=COLORS["red"], linestyle="--",
                    label=f"Median: {np.median(durations):.0f}m")
        ax.set_title("CI Run Duration Distribution (successful runs)")
        ax.set_xlabel("Minutes")
        ax.set_ylabel("Count")
        ax.legend()
        save(fig, "26_ci_duration_distribution")

    # ── Chart: Failure rate by workflow name ──
    name_success = Counter()
    name_fail = Counter()
    for r in runs:
        if r["conclusion"] == "success":
            name_success[r["name"]] += 1
        elif r["conclusion"] == "failure":
            name_fail[r["name"]] += 1

    wf_names = [n for n in name_counts if name_counts[n] >= 20]
    if wf_names:
        fail_rates = []
        for n in wf_names:
            total = name_success.get(n, 0) + name_fail.get(n, 0)
            fail_rates.append(name_fail.get(n, 0) / total * 100 if total else 0)
        sorted_pairs = sorted(zip(wf_names, fail_rates), key=lambda x: x[1], reverse=True)
        fig, ax = plt.subplots()
        ax.barh([p[0] for p in sorted_pairs][::-1],
                [p[1] for p in sorted_pairs][::-1],
                color=COLORS["red"])
        ax.set_title("CI Failure Rate by Workflow (min 20 runs)")
        ax.set_xlabel("Failure Rate (%)")
        save(fig, "27_ci_failure_by_workflow")


def report_cross_cutting(f, conn, prs, issues, commits, runs):
    section_header(f, "CROSS-CUTTING ANALYSIS", 8)

    # Overall project velocity
    activity_by_month = Counter()
    for pr in prs:
        dt = parse_dt(pr["created_at"])
        if dt:
            activity_by_month[month_key(dt)] += 1
    for iss in issues:
        dt = parse_dt(iss["created_at"])
        if dt:
            activity_by_month[month_key(dt)] += 1
    for c in commits:
        dt = parse_dt(c["date"])
        if dt:
            activity_by_month[month_key(dt)] += 1

    months = sorted(activity_by_month)
    stat(f, 48, "Avg monthly activity (PRs + issues + commits)",
         f"{np.mean(list(activity_by_month.values())):.0f}" if activity_by_month else "N/A")

    # Contributor overlap
    pr_authors = set(pr["user_login"] for pr in prs if pr["user_login"])
    issue_authors = set(iss["user_login"] for iss in issues if iss["user_login"])
    commit_authors = set(c["author_login"] for c in commits if c["author_login"])

    all_contributors = pr_authors | issue_authors | commit_authors
    all_three = pr_authors & issue_authors & commit_authors
    stat(f, 49, "Total unique contributors (all types)", str(len(all_contributors)))
    stat(f, 50, "Contributors active in all 3 areas", str(len(all_three)))

    # ── Chart: Project velocity ──
    pr_monthly = Counter()
    issue_monthly = Counter()
    commit_monthly = Counter()
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

    fig, ax = plt.subplots()
    x = range(len(months))
    ax.bar(x, [commit_monthly.get(m, 0) for m in months],
           label="Commits", color=COLORS["green"])
    ax.bar(x, [pr_monthly.get(m, 0) for m in months],
           bottom=[commit_monthly.get(m, 0) for m in months],
           label="PRs", color=COLORS["blue"])
    ax.bar(x, [issue_monthly.get(m, 0) for m in months],
           bottom=[commit_monthly.get(m, 0) + pr_monthly.get(m, 0) for m in months],
           label="Issues", color=COLORS["orange"])
    ax.set_xticks(list(x))
    ax.set_xticklabels(months, rotation=45, ha="right")
    ax.set_title("Overall Project Velocity (Stacked)")
    ax.set_ylabel("Activity Count")
    ax.legend()
    save(fig, "28_project_velocity")

    # ── Chart: Contributor Venn-ish breakdown ──
    pr_only = pr_authors - issue_authors - commit_authors
    issue_only = issue_authors - pr_authors - commit_authors
    commit_only = commit_authors - pr_authors - issue_authors
    pr_issue = (pr_authors & issue_authors) - commit_authors
    pr_commit = (pr_authors & commit_authors) - issue_authors
    issue_commit = (issue_authors & commit_authors) - pr_authors

    fig, ax = plt.subplots(figsize=(10, 6))
    categories = [
        "PR only", "Issue only", "Commit only",
        "PR + Issue", "PR + Commit", "Issue + Commit",
        "All three"
    ]
    sizes = [
        len(pr_only), len(issue_only), len(commit_only),
        len(pr_issue), len(pr_commit), len(issue_commit),
        len(all_three)
    ]
    bars = ax.barh(categories[::-1], sizes[::-1],
                   color=[COLORS["blue"], COLORS["green"], COLORS["orange"],
                          COLORS["purple"], COLORS["brown"], COLORS["pink"],
                          COLORS["red"]][::-1])
    ax.set_title("Contributor Activity Overlap")
    ax.set_xlabel("Contributors")
    for bar, val in zip(bars, sizes[::-1]):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                str(val), va="center")
    save(fig, "29_contributor_overlap")

    # ── Chart: Weekend vs weekday activity heatmap ──
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
    ax.set_xticklabels([f"{h}" for h in range(24)])
    ax.set_xlabel("Hour (UTC)")
    ax.set_title("Activity Heatmap: Day of Week vs Hour (PRs + Commits)")
    fig.colorbar(im, ax=ax, label="Activity Count")
    save(fig, "30_activity_heatmap")


def main():
    conn = connect()

    prs = load_prs(conn)
    issues = load_issues(conn)
    commits = load_commits(conn)
    runs = load_runs(conn)

    report_path = OUT_DIR / "report.txt"
    with open(report_path, "w") as f:
        f.write("=" * 70 + "\n")
        f.write("  BITCOIN CORE (bitcoin/bitcoin) — GITHUB STATISTICS REPORT\n")
        f.write(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"  Data: ~12 months of GitHub activity\n")
        f.write("=" * 70 + "\n")

        report_overview(f, conn)
        report_pr_activity(f, conn, prs)
        report_pr_authors(f, conn, prs)
        report_pr_timing(f, prs)
        report_issues(f, conn, issues)
        report_commits(f, conn, commits)
        report_ci(f, conn, runs)
        report_cross_cutting(f, conn, prs, issues, commits, runs)

        f.write(f"\n{'='*70}\n")
        f.write(f"  Charts saved to: {OUT_DIR.resolve()}/\n")
        f.write(f"{'='*70}\n")

    conn.close()

    with open(report_path) as f:
        print(f.read())

    print(f"\n{len(list(OUT_DIR.glob('*.png')))} charts generated in {OUT_DIR.resolve()}/")


if __name__ == "__main__":
    main()
