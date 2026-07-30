# Zone Universe

Local, resumable ingestion and website search for approved ICANN CZDS zone
files.

The source `.txt.gz` files are retained. Delegated domains are streamed into
five priority passes across 64 lexical shards (320 total), each with byte-offset
checkpoints. New delegations and plausible company names run first without
discarding the long tail. Obvious generated labels are excluded with a
conservative, auditable quality gate. Successfully fetched websites are indexed
in SQLite FTS5, while exact-duplicate raw HTML shares a compressed archive blob
outside the database.

This project does not bypass CZDS approval. `.com` and `.app` downloads work
only after their registry operators approve the ICANN CZDS requests.

## Quick start

```zsh
cd "/Users/ryandevlin/Desktop/HTTPS Scraping/zone-universe"
export CZDS_USERNAME='your-email@example.com'
export PYTHONPATH="$PWD/src:../ai-domain-search-engine/src"

./czds.py auth
./czds.py list
./czds.py download app com -o data/zonefiles

./zone_cli.sh init

# Import all approved .app delegations.
./zone_cli.sh import-zone \
  data/zonefiles/app.txt.gz --tld app

# Start with a 100,000-domain .com pilot.
./zone_cli.sh import-zone \
  data/zonefiles/com.txt.gz --tld com --limit 100000

./start_worker.sh
./zone_cli.sh stats

# Manually enrich up to ten selected registration dates through registry RDAP.
./zone_cli.sh registration \
  example.com example.app
```

Stop and resume safely:

```zsh
./stop_worker.sh
./start_worker.sh
```

The crawler resumes from the committed byte offset in every queue shard.

## What happens to each domain

1. Import validates the zone record and permanently excludes only obvious
   generated labels.
2. A cold-priority gate bypasses low-value legacy names before DNS or HTTP.
   They remain recoverable in the source queue; newly delegated names bypass
   this gate and are always checked.
3. DNS preflight skips names with no apex or `www` address.
4. The crawler fetches the homepage first. Parked, placeholder, prelaunch, and
   obvious junk sites stop after that retained page; plausible companies
   receive one or two high-value evidence pages.
5. Extracted content enters exact-phrase FTS search.
6. Raw HTML enters a compressed append-only archive.
7. Temporary timeouts, rate limits and 5xx responses receive bounded backoff.
8. Successful domains are classified immediately by the original Glasswing
   Python model from the local content cache.
9. Investable and Hard Denied results enter Yes and No respectively, while all
   buckets remain manually movable from the Zone Universe page.
10. Each later zone import writes sorted `added.txt.gz` and `removed.txt.gz`
   deltas; newly delegated domains enter the first crawl pass.

The worker watches `data/zonefiles/` for completed atomic CZDS downloads. A new
`.txt.gz` file is imported automatically; `.part` files are ignored. Previously
successful domains are skipped after a zone refresh instead of being fetched
again.

Generated-domain audit files are compressed under `data/rejected/`. The
original ICANN zone files are never modified.

The default active crawl includes quality priorities 0–2 and cold-quarantines
priority 3. To tighten or widen that recoverable gate, set a value from 0 to 3
before starting the worker:

```zsh
export ZONE_UNIVERSE_MAX_CRAWL_PRIORITY=2
```

Successfully imported CZDS downloads are temporary by default. Once the
prioritized queue, compressed snapshot, and import metadata commit
successfully, the original multi-gigabyte download is deleted:

```zsh
export ZONE_UNIVERSE_DELETE_IMPORTED_ZONEFILES=1
```

Set this to `0` only when you explicitly want to retain the re-downloadable
raw zone file. The compressed queue snapshot remains available for future
delegation diffs.

Rejected generated-domain audits are stored as compact JSON summaries with
reason counts and representative examples, not millions of rejected domain
rows. Older TSV audits can be converted safely with:

```zsh
./zone_cli.sh compact-rejection-audits
```

Remove any already-crawled domains that fail the current quality gate:

```zsh
./zone_cli.sh prune-noise
```

## Scale and storage

The `.com` zone is enormous. A complete crawl is a distributed-scale workload,
not a quick laptop job. The local worker therefore defaults to bounded
concurrency, stores only successful website HTML, reuses completed pages after
zone refreshes, and pauses before available disk falls below 20 GB.

Use the `.app` import first and a 100,000-domain `.com` pilot before deciding
whether to import the full `.com` queue.

To retain the complete HTML archive on an external disk, set this before
starting the worker:

```zsh
export ZONE_UNIVERSE_HTML_ARCHIVE="/Volumes/YourDisk/zone-html"
./start_worker.sh
```

The dashboard estimates remaining storage from observed compression and shows
the configured 20 GB safety reserve.
