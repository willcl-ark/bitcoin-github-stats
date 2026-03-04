# gh-stats

GitHub repository statistics collector and dashboard for [bitcoin/bitcoin](https://github.com/bitcoin/bitcoin). Fetches PRs, issues, commits, and CI workflow runs into a local SQLite database, then generates an interactive report as a static site.

## Architecture

```
Rust CLI (gh-stats)          Python (analyze.py)         Browser
  fetch-day / backfill         reads SQLite DB              loads data.json
  → GitHub REST API            → emits site/data.json       → Chart.js renders
  → SQLite DB                                                → date range picker
```

Nightly GitHub Actions workflow: download DB from release asset → fetch yesterday → generate JSON → upload DB → deploy to GitHub Pages.

## Setup

Requires a Nix devShell (`nix develop`) or manually: Rust toolchain, Python 3, and `just`.

Authentication: set `GITHUB_TOKEN` or have `gh auth` configured.

## Usage

### Backfill historical data

```sh
cargo run --release -- backfill --from 2012-01-01 --resume
```

`--resume` picks up where a previous run left off. PRs and issues paginate through all records (~5 min for bitcoin/bitcoin). Workflow runs are clamped to the last 400 days (GitHub's retention limit). Commits use the `since`/`until` API parameters.

### Daily incremental fetch

```sh
cargo run --release -- fetch-day --date 2026-03-03
```

Omit `--date` to fetch today.

### Generate the report

```sh
just generate    # runs analyze.py → site/data.json
just serve       # local server at http://localhost:8000
just dev         # generate + serve
```

### Deploy

```sh
just sync        # rsync site/ to seedbox mirror
```

GitHub Pages deployment happens automatically via the nightly workflow, or manually with `gh workflow run update-stats.yml`.

## Data flow

- **`gh-stats.db`** — SQLite database with tables: `pull_requests`, `issues`, `commits`, `workflow_runs`, `sync_state`, `sync_log`
- **`site/data.json`** — Pre-aggregated per-month statistics (~100KB gzipped). All chart data, author counts, timing distributions, etc.
- **`site/index.html`** — Self-contained static page. Fetches `data.json`, aggregates selected date range client-side, renders with Chart.js.

The DB is stored as a GitHub Release asset (`db-latest`) for persistence between CI runs.
