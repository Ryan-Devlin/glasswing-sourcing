#!/usr/bin/env python3
"""ICANN CZDS (Centralized Zone Data Service) API client.

Implements the CZDS REST API v1.0.9 (ICANN_CZDS_api.pdf) using only the
standard library.

    POST https://account-api.icann.org/api/authenticate  -> {"accessToken": JWT}
    GET  https://czds-api.icann.org/czds/downloads/links  -> [download urls]
    GET  <download_url>/<tld>.zone                        -> gzipped zone file
    HEAD <download_url>/<tld>.zone                        -> size/mtime/filename

Notes on the API that shape this client:
  * There is NO endpoint for requesting access to a TLD. Requests are submitted
    and approved in the CZDS portal (https://czds.icann.org); the API only
    exposes zones already approved for your account.
  * A valid User-Agent is REQUIRED. Without one ICANN may redirect you to a
    maintenance page, which looks like a corrupt download rather than an error.
  * Authentication is rate limited to 8 attempts per 5 minutes per IP address,
    and tokens last 24h -- so tokens are cached on disk.
"""

import argparse
import base64
import concurrent.futures
import getpass
import gzip
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

AUTH_URL = os.environ.get("CZDS_AUTH_URL", "https://account-api.icann.org/api/authenticate")
BASE_URL = os.environ.get("CZDS_BASE_URL", "https://czds-api.icann.org")
USER_AGENT = os.environ.get("CZDS_USER_AGENT", "czds-cli/1.0 (+https://czds.icann.org)")

STATE_DIR = os.path.expanduser(os.environ.get("CZDS_STATE_DIR", "~/.czds"))
TOKEN_CACHE = os.path.join(STATE_DIR, "token.json")
KEYCHAIN_SERVICE = "icann-czds"

# The token is refreshed this many seconds before its exp claim.
EXPIRY_SKEW = 300
RETRY_STATUSES = (500, 502, 503, 504)


class CzdsError(Exception):
    pass


def log(msg):
    print("%s  %s" % (datetime.now().strftime("%H:%M:%S"), msg), file=sys.stderr, flush=True)


# ---------------------------------------------------------------- credentials


def load_credentials(args):
    """Resolve credentials from --username/env, then keychain, then config, then prompt."""
    username = args.username or os.environ.get("CZDS_USERNAME")
    password = os.environ.get("CZDS_PASSWORD")

    config = {}
    if os.path.exists(args.config):
        with open(args.config) as fh:
            config = json.load(fh)
    username = username or config.get("icann.account.username")
    password = password or config.get("icann.account.password")

    if username and not password:
        password = keychain_get(username)

    if not username:
        username = input("CZDS username (email): ").strip()
    if not password:
        password = keychain_get(username) or getpass.getpass("CZDS password: ")

    if not username or not password:
        raise CzdsError("no credentials; see README for CZDS_USERNAME / CZDS_PASSWORD")
    return username, password


def keychain_get(account):
    """Read the password from the macOS keychain, or None if unavailable."""
    if sys.platform != "darwin" or not account:
        return None
    try:
        out = subprocess.run(
            ["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", account, "-w"],
            capture_output=True, text=True, timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None if out.returncode == 0 else None


def keychain_set(account, password):
    subprocess.run(
        ["security", "add-generic-password", "-U", "-s", KEYCHAIN_SERVICE,
         "-a", account, "-w", password],
        check=True,
    )


# ---------------------------------------------------------------------- HTTP


def request(url, method="GET", body=None, token=None, timeout=120):
    """Perform one HTTP request. Returns the open response; caller must close it."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    return urllib.request.urlopen(req, timeout=timeout)


def request_with_retry(url, method="GET", body=None, token=None, timeout=120, attempts=4):
    """Retry transient 5xx and network failures with exponential backoff.

    HTTP errors that carry meaning (401/403/409/429) are raised for the caller
    to interpret rather than retried blindly.
    """
    delay = 2.0
    for attempt in range(1, attempts + 1):
        try:
            return request(url, method, body, token, timeout)
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRY_STATUSES or attempt == attempts:
                raise
            log("HTTP %d from %s, retrying in %.0fs" % (exc.code, url, delay))
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == attempts:
                raise
            log("%s on %s, retrying in %.0fs" % (exc, url, delay))
        time.sleep(delay)
        delay *= 2
    raise CzdsError("unreachable")


# ------------------------------------------------------------------- session


class Session:
    """Holds an access token, refreshing it on expiry or 401.

    Thread safe: parallel downloads share one token and one refresh.
    """

    def __init__(self, username, password, no_cache=False):
        self.username = username
        self.password = password
        self.no_cache = no_cache
        self._token = None
        self._exp = 0
        self._lock = threading.Lock()
        if not no_cache:
            self._load_cache()

    # -- token cache -----------------------------------------------------

    def _load_cache(self):
        try:
            with open(TOKEN_CACHE) as fh:
                cached = json.load(fh)
        except (OSError, ValueError):
            return
        if cached.get("username") != self.username:
            return
        if cached.get("exp", 0) - EXPIRY_SKEW > time.time():
            self._token, self._exp = cached["accessToken"], cached["exp"]
            log("using cached token (expires %s)" % fmt_time(self._exp))

    def _save_cache(self):
        if self.no_cache:
            return
        os.makedirs(STATE_DIR, mode=0o700, exist_ok=True)
        tmp = TOKEN_CACHE + ".tmp"
        with open(tmp, "w") as fh:
            os.chmod(tmp, 0o600)
            json.dump({"username": self.username, "accessToken": self._token,
                       "exp": self._exp}, fh)
        os.replace(tmp, TOKEN_CACHE)

    # -- auth ------------------------------------------------------------

    def authenticate(self):
        """POST /api/authenticate. Mind the 8-per-5-minute IP rate limit."""
        log("authenticating %s" % self.username)
        try:
            with request_with_retry(
                AUTH_URL, "POST",
                {"username": self.username, "password": self.password},
            ) as resp:
                payload = json.load(resp)
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise CzdsError(
                    "invalid credentials -- reset the password at https://czds.icann.org "
                    "(there is no API for credential management)")
            if exc.code == 429:
                raise CzdsError(
                    "rate limited: the auth endpoint allows 8 attempts per 5 minutes per "
                    "IP. Wait 5 minutes; do not loop on this.")
            raise CzdsError("authentication failed: HTTP %d %s" % (exc.code, exc.reason))

        self._token = payload["accessToken"]
        self._exp = jwt_exp(self._token) or (time.time() + 23 * 3600)
        self._save_cache()
        log("token acquired, expires %s" % fmt_time(self._exp))
        return self._token

    @property
    def token(self):
        with self._lock:
            if not self._token or self._exp - EXPIRY_SKEW <= time.time():
                self.authenticate()
            return self._token

    def invalidate(self, stale):
        """Force a refresh, unless another thread already replaced `stale`."""
        with self._lock:
            if self._token == stale:
                self._token, self._exp = None, 0

    # -- authorized requests ---------------------------------------------

    def get(self, url, method="GET", timeout=120):
        """Authorized request that re-authenticates once on 401."""
        for final in (False, True):
            token = self.token
            try:
                return request_with_retry(url, method, token=token, timeout=timeout)
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and not final:
                    log("token rejected, re-authenticating")
                    self.invalidate(token)
                    continue
                raise translate_zone_error(exc, url)
        raise CzdsError("unreachable")

    def links(self):
        """GET /czds/downloads/links -> list of authorized zone download URLs."""
        with self.get(BASE_URL + "/czds/downloads/links") as resp:
            return json.load(resp)


def translate_zone_error(exc, url):
    """Turn documented CZDS status codes into actionable messages."""
    if exc.code == 403:
        return CzdsError(
            "%s: 403 -- zone does not exist, or your account has no approved request "
            "for it. Check https://czds.icann.org." % url)
    if exc.code == 409:
        return CzdsError(
            "%s: 409 -- you must accept the new Terms & Conditions at "
            "https://czds.icann.org before the API will serve zone files." % url)
    if exc.code == 401:
        return CzdsError("%s: 401 -- token rejected after refresh" % url)
    return CzdsError("%s: HTTP %d %s" % (url, exc.code, exc.reason))


def jwt_exp(token):
    """Read the exp claim out of a JWT payload without verifying the signature."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return int(json.loads(base64.urlsafe_b64decode(payload))["exp"])
    except (IndexError, ValueError, KeyError, TypeError):
        return None


def fmt_time(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# --------------------------------------------------------------------- zones


def tld_of(url):
    """https://.../downloads/example.zone -> example"""
    return url.rsplit("/", 1)[-1][: -len(".zone")]


def select_links(links, tlds):
    """Filter links to the requested TLDs, erroring on ones that aren't approved."""
    if not tlds:
        return sorted(links, key=tld_of)
    by_tld = {tld_of(u): u for u in links}
    missing = [t for t in tlds if t not in by_tld]
    if missing:
        raise CzdsError(
            "not approved for your account: %s\nApproved zones: %s"
            % (", ".join(missing), ", ".join(sorted(by_tld)) or "(none)"))
    return [by_tld[t] for t in tlds]


def head(session, url):
    """HEAD a zone URL -> {filename, size, last_modified}."""
    with session.get(url, method="HEAD", timeout=60) as resp:
        headers = resp.headers
    disposition = headers.get("Content-Disposition", "")
    filename = ""
    for part in disposition.split(";"):
        if part.strip().lower().startswith("filename"):
            filename = part.split("=", 1)[-1].strip().strip('"')
    return {
        "tld": tld_of(url),
        "url": url,
        "filename": filename or tld_of(url) + ".txt.gz",
        "size": int(headers.get("Content-Length") or 0),
        "last_modified": headers.get("Last-Modified", ""),
    }


def human(size):
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return "%.1f%s" % (size, unit) if unit != "B" else "%dB" % size
        size /= 1024.0


# ------------------------------------------------------------------ download


def state_path(outdir):
    return os.path.join(outdir, ".czds-state.json")


def read_state(outdir):
    try:
        with open(state_path(outdir)) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def write_state(outdir, state, lock):
    with lock:
        tmp = state_path(outdir) + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(state, fh, indent=2, sort_keys=True)
        os.replace(tmp, state_path(outdir))


def download_one(session, url, outdir, state, lock, force, decompress):
    """HEAD, skip if unchanged, then stream the zone to disk atomically."""
    tld = tld_of(url)
    meta = head(session, url)
    dest = os.path.join(outdir, meta["filename"])
    if decompress and dest.endswith(".gz"):
        dest = dest[: -len(".gz")]

    prior = state.get(tld, {})
    if (not force and os.path.exists(dest)
            and prior.get("last_modified") == meta["last_modified"]
            and prior.get("size") == meta["size"]):
        log("%-16s unchanged since %s, skipping" % (tld, meta["last_modified"]))
        return {"tld": tld, "status": "skipped", "path": dest, "bytes": 0}

    tmp = dest + ".part"
    log("%-16s downloading %s" % (tld, human(meta["size"]) if meta["size"] else "?"))
    written = 0
    try:
        with session.get(url, timeout=1800) as resp, open(tmp, "wb") as out:
            stream = gzip.GzipFile(fileobj=resp) if decompress else resp
            shutil.copyfileobj(stream, out, 1024 * 256)
            written = out.tell()

        # A truncated transfer is worse than a failed one: it looks like a
        # valid zone file. Only trust the length check on the raw
        # (non-decompressed) path.
        if not decompress and meta["size"] and written != meta["size"]:
            raise CzdsError("%s: expected %d bytes, got %d"
                            % (tld, meta["size"], written))
    except BaseException:
        # Never leave the partial behind. A multi-GB .part from an aborted
        # transfer is what turns one out-of-space failure into a volume that
        # stays full and fails every retry after it.
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

    os.replace(tmp, dest)
    state[tld] = {"last_modified": meta["last_modified"], "size": meta["size"],
                  "filename": os.path.basename(dest),
                  "downloaded": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    write_state(outdir, state, lock)
    log("%-16s wrote %s -> %s" % (tld, human(written), dest))
    return {"tld": tld, "status": "downloaded", "path": dest, "bytes": written}


# ------------------------------------------------------------------ commands


def cmd_auth(session, args):
    session.authenticate()
    print("OK: authenticated as %s\ntoken expires %s\ncached at %s"
          % (session.username, fmt_time(session._exp),
             "(disabled)" if session.no_cache else TOKEN_CACHE))


def cmd_list(session, args):
    links = session.links()
    if args.json:
        print(json.dumps(sorted(links), indent=2))
        return
    if not links:
        print("No approved zones. Submit requests at https://czds.icann.org "
              "-- there is no API for this.")
        return
    for url in sorted(links, key=tld_of):
        print(tld_of(url))
    print("\n%d approved zone(s)" % len(links), file=sys.stderr)


def cmd_status(session, args):
    links = select_links(session.links(), args.tlds)
    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for meta in pool.map(lambda u: head(session, u), links):
            rows.append(meta)
    rows.sort(key=lambda r: r["tld"])
    if args.json:
        print(json.dumps(rows, indent=2))
        return
    print("%-20s %10s  %-31s %s" % ("TLD", "SIZE", "LAST MODIFIED", "FILENAME"))
    for r in rows:
        print("%-20s %10s  %-31s %s"
              % (r["tld"], human(r["size"]), r["last_modified"], r["filename"]))
    print("\ntotal %s across %d zone(s)"
          % (human(sum(r["size"] for r in rows)), len(rows)), file=sys.stderr)


def cmd_download(session, args):
    links = select_links(session.links(), args.tlds)
    outdir = os.path.abspath(args.output)
    os.makedirs(outdir, exist_ok=True)
    state, lock = read_state(outdir), threading.Lock()
    log("%d zone(s) -> %s" % (len(links), outdir))

    results, failures = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = {
            pool.submit(download_one, session, url, outdir, state, lock,
                        args.force, args.decompress): url
            for url in links
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:  # keep going; report every failure at the end
                failures.append((tld_of(futures[future]), exc))
                log("FAILED %s: %s" % (tld_of(futures[future]), exc))

    got = [r for r in results if r["status"] == "downloaded"]
    print("\ndownloaded %d (%s), skipped %d, failed %d"
          % (len(got), human(sum(r["bytes"] for r in got)),
             len(results) - len(got), len(failures)))
    for tld, exc in failures:
        print("  FAILED %s: %s" % (tld, exc), file=sys.stderr)
    return 1 if failures else 0


def cmd_store(session, args):
    keychain_set(session.username, session.password)
    print("Stored password for %s in the macOS keychain (service %r)."
          % (session.username, KEYCHAIN_SERVICE))


# ---------------------------------------------------------------------- main


def build_parser():
    parser = argparse.ArgumentParser(
        prog="czds", description="ICANN CZDS API client.",
        epilog="Access to a TLD must be requested and approved at "
               "https://czds.icann.org; the API only serves approved zones.")
    parser.add_argument("-u", "--username", help="CZDS account email (or $CZDS_USERNAME)")
    parser.add_argument("-c", "--config", default="config.json",
                        help="JSON config file (default: config.json)")
    parser.add_argument("--no-token-cache", action="store_true",
                        help="always re-authenticate; mind the 8/5min rate limit")
    parser.add_argument("-j", "--jobs", type=int, default=4,
                        help="parallel requests (default: 4)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("auth", help="verify credentials and cache a token")
    p.set_defaults(func=cmd_auth)

    p = sub.add_parser("list", help="list zones approved for your account")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("status", help="size / last-modified per zone (HEAD, no download)")
    p.add_argument("tlds", nargs="*", help="default: all approved zones")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("download", help="download zone files")
    p.add_argument("tlds", nargs="*", help="default: all approved zones")
    p.add_argument("-o", "--output", default="zonefiles", help="output dir (default: zonefiles)")
    p.add_argument("--force", action="store_true", help="re-download unchanged zones")
    p.add_argument("--decompress", action="store_true", help="write plain text, not .gz")
    p.set_defaults(func=cmd_download)

    p = sub.add_parser("store-credentials", help="save the password to the macOS keychain")
    p.set_defaults(func=cmd_store)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        username, password = load_credentials(args)
        session = Session(username, password, no_cache=args.no_token_cache)
        return args.func(session, args) or 0
    except CzdsError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
