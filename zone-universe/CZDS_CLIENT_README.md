# ICANN CZDS API client

Single-file, stdlib-only Python client for the ICANN Centralized Zone Data Service
REST API (spec v1.0.9). Downloads the gTLD zone files your account is approved for.

## Before the API works

The CZDS API is **download-only**. There is no endpoint for requesting access to a
TLD — that happens in the portal, and until a request is *approved* the API will
show you nothing.

1. Create an account at <https://czds.icann.org>.
2. Accept the Terms & Conditions.
3. Submit zone requests (individual TLDs, or "request all"). Approval is per-TLD
   and per-registry — some are automatic, some take days, some are refused.
4. Requests expire annually and T&Cs get revised; both require a portal visit.
   `409` from the API means new T&Cs are waiting.

## Setup

No dependencies, Python 3.8+.

```sh
chmod +x czds.py
```

Credentials resolve in this order: `--username` / `$CZDS_USERNAME` and
`$CZDS_PASSWORD` → macOS keychain → `config.json` → interactive prompt.

Recommended (keychain, nothing on disk in the clear):

```sh
export CZDS_USERNAME='you@example.com'
./czds.py store-credentials      # prompts, saves to keychain service "icann-czds"
./czds.py auth                   # verify
```

Or `cp config.sample.json config.json` and fill it in — it's gitignored and should
be `chmod 600`.

## Usage

```sh
./czds.py auth                        # verify credentials, cache a 24h token
./czds.py list                        # TLDs approved for your account
./czds.py status                      # size + last-modified per zone (HEAD only)
./czds.py status com net --json
./czds.py download                    # all approved zones -> ./zonefiles/
./czds.py download xyz top -o /data   # specific zones
./czds.py download --decompress       # plain text instead of .gz
./czds.py download --force            # ignore the unchanged check
./czds.py download -j 8               # more parallelism
```

`download` records each zone's `Last-Modified` and size in
`<outdir>/.czds-state.json` and skips zones that haven't changed, so it's safe to
run on a cron. Zones publish roughly daily; ICANN asks that you not poll harder
than that.

## API notes

Endpoints, per the spec:

| Call | Purpose |
| --- | --- |
| `POST https://account-api.icann.org/api/authenticate` | `{username, password}` → `{accessToken}` (JWT, 24h) |
| `GET https://czds-api.icann.org/czds/downloads/links` | JSON array of authorized zone URLs |
| `GET <download_url>/<tld>.zone` | the zone file, gzipped |
| `HEAD <download_url>/<tld>.zone` | `Content-Length`, `Last-Modified`, `Content-Disposition` |

Behaviors this client works around:

- **`User-Agent` is mandatory.** Omit it and ICANN may redirect you to a
  maintenance page — you get HTML that looks like a corrupt zone file rather than
  an error. Override with `$CZDS_USER_AGENT`.
- **Auth is rate limited to 8 attempts per 5 minutes, per IP.** Tokens are cached
  in `~/.czds/token.json` (mode 600) and reused until 5 minutes before the JWT
  `exp` claim. Don't run with `--no-token-cache` in a loop.
- **`403`** = zone doesn't exist *or* you have no approved request for it.
  **`409`** = accept the new T&Cs in the portal. Both are terminal, not retryable.
- Downloads stream to `<file>.part`, verify the byte count against
  `Content-Length`, then atomically rename — a truncated transfer never lands as a
  plausible-looking zone file.
- Transient `5xx` and network errors retry with exponential backoff; a `401`
  mid-run triggers one re-auth and a retry.
- The API is **IPv4-only** and HTTPS-only.

Zone files are RFC 1035 master-file format: `NS`/`DS`/glue records for the
delegations in that TLD. They are not a list of registrant data, and CZDS terms
restrict how you may use and redistribute them.

## Reference

- Portal: <https://czds.icann.org>
- Spec PDF: <https://github.com/icann/czds-api-client-java/blob/master/docs/ICANN_CZDS_api.pdf>
- ICANN reference clients: [python](https://github.com/icann/czds-api-client-python), [java](https://github.com/icann/czds-api-client-java)
