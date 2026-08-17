# Zone Universe

Local, resumable ingestion and website search for approved ICANN CZDS zone
files.

Each daily zone file becomes a compact comparison snapshot. The first import
establishes a baseline and queues nothing; later imports stream-compute
`today - previous snapshot` and put only newly delegated domains into 64
byte-addressed shards. Obvious generated labels are excluded with a
conservative, auditable quality gate.

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

# Establish the first .app baseline (queues zero historical domains).
./zone_cli.sh import-zone \
  data/zonefiles/app.txt.gz --tld app

# Establish a .com baseline.
./zone_cli.sh import-zone \
  data/zonefiles/com.txt.gz --tld com

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
2. A streaming set difference selects only domains added since the previous
   daily snapshot. Historical, unchanged, and removed names are not queued.
3. DNS preflight skips names with no apex or `www` address.
4. A capped 128 KB homepage preflight reads metadata and opening copy. Parked,
   placeholder, low-value, and unrelated sites stop here without HTML storage,
   search indexing, RDAP, or Python classification.
5. Only sites with credible technology-company evidence receive a full
   homepage crawl and, when useful, one or two high-value evidence pages.
6. Retained content enters exact-phrase FTS search and raw HTML enters a
   compressed append-only archive.
7. Temporary timeouts, rate limits and 5xx responses receive bounded backoff.
8. Successful domains are classified immediately by the original Glasswing
   Python model from the local content cache.
9. Independent Yes/No training labels can be added inline without changing the
   company's Python classification bucket.
10. Each later zone import writes sorted `added.txt.gz` and `removed.txt.gz`
    deltas; only newly delegated domains enter the crawl pass.

The worker watches `data/zonefiles/` for completed atomic CZDS downloads. A new
`.txt.gz` file is imported automatically; `.part` files are ignored. The first
file for a TLD is a baseline. Every later file queues only additions.

Generated-domain audit files are compressed under `data/rejected/`. The
original ICANN zone files are never modified.

The inexpensive homepage gate is on by default. These optional settings tune
its request cap and concurrency without changing the full-crawl worker count:

```zsh
export ZONE_UNIVERSE_PREFLIGHT_ENABLED=1
export ZONE_UNIVERSE_PREFLIGHT_MAX_RESPONSE_BYTES=131072
export ZONE_UNIVERSE_PREFLIGHT_CONCURRENCY=100
```

The default checkpoint batch is 100 domains so a safe pause is acknowledged
quickly. Set `ZONE_UNIVERSE_BATCH_SIZE` if the deployment needs a different
latency/throughput balance.

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
