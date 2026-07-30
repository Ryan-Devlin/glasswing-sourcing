# AnySite AI Search

A standalone private search engine for the actual content of `.ai` websites.
It does not use or modify any other project.

Search for anything found in crawled website text:

```text
robotics
"drug discovery"
supply chain
cybersecurity -consulting
```

Searches use SQLite FTS5—not domain-name matching or slow `%LIKE%` scans.

## Compact storage

The searchable database stores extracted text, metadata, and the FTS keyword
index. Raw HTML is never stored directly in SQLite. Useful pages are written
to deduplicated, zlib-compressed shard archives under `data/html`; parked and
prelaunch templates retain searchable evidence but not their source HTML.
This preserves exact-phrase search while keeping the hot database small.

Configure the archive with:

```text
AI_SEARCH_HTML_ARCHIVE=data/html
AI_SEARCH_RETAIN_RAW_HTML=1
```

To migrate an older database that still contains inline HTML:

```zsh
.venv/bin/python -m ai_search.cli compact-storage
```

The migration creates a compressed safety backup, verifies every newly
written archive block before removing the inline copy, and vacuums SQLite
afterward. It is resumable if interrupted.

When a search is submitted in the dashboard, it is saved as a persistent watch.
The separate background worker continuously:

1. submits the watched term to a broad web-search index using `site:.ai`;
2. adds them to the local crawl queue;
3. fetches and indexes their public website content;
4. makes new matches available to every saved watch.

By default the app attempts a keyless DuckDuckGo HTML fallback, which may be
rate-limited or return no results. For broad, reliable API coverage, create a
Brave Search API key and put this in `.env`:

```text
AI_SEARCH_WEB_PROVIDER=brave
BRAVE_SEARCH_API_KEY=your_key_here
AI_SEARCH_PROVIDER_PAGES=10
```

Bing is also supported with `AI_SEARCH_WEB_PROVIDER=bing` and
`BING_SEARCH_API_KEY`. Search providers limit result depth, so no configuration
can guarantee every website on the internet. Bulk domain imports remain
available for additional coverage.

## Setup

```zsh
cd "/Users/ryandevlin/Desktop/HTTPS Scraping/ai-domain-search-engine"
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
```

## Import and crawl domains

Import a text or CSV file (CSV columns may be `domain`, `registrable`, or
`hostname`):

```zsh
.venv/bin/python -m ai_search.cli import domains.txt
.venv/bin/python -m ai_search.cli crawl --limit 100
```

Add domains directly:

```zsh
.venv/bin/python -m ai_search.cli add example.ai another.ai
```

## Search from the terminal

```zsh
.venv/bin/python -m ai_search.cli search 'robotics -education'
.venv/bin/python -m ai_search.cli search '"quantum computing"'
```

## Launch the dashboard

```zsh
./run.sh
```

Then open <http://localhost:8502>.

`run.sh` starts the persistent discovery/crawl worker if needed. The worker
continues after the dashboard closes. Control it directly with:

```zsh
./start_worker.sh
./stop_worker.sh
tail -f logs/worker.log
```

## Test

```zsh
.venv/bin/python -m pytest -q
```

The database defaults to `data/search.sqlite3`. Configuration lives in `.env`;
no secrets are hard-coded.
