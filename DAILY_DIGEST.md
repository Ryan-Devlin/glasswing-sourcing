# Daily sourcing digest

`daily_digest.py` sends a low-noise email containing newly classified,
high-confidence Investable companies. It is designed to run as a short-lived
Railway cron job and exits after every run.

Default safeguards:

- Investable (`priority`) bucket only.
- Glasswing fit score of 75 or higher.
- Classification confidence of 80 or higher.
- Only companies updated in the preceding 24 hours.
- Hidden and Mark No companies are excluded.
- A domain is delivered only once to each recipient.
- A maximum of 20 companies per email.
- Empty digests do not send email.

## Preview locally

```bash
.venv/bin/python daily_digest.py --dry-run
```

## Railway configuration

Create a service from this repository named `daily-digest`, set its config file
path to `/railway.digest.json`, and give it the same private database reference
used by the web service. The committed schedule is `0 13 * * *` (13:00 UTC,
approximately 9:00 AM Eastern during daylight saving time).

Required variables:

```text
CLASSIFICATION_DATABASE_URL=${{Postgres.DATABASE_URL}}
DIGEST_EMAIL_TO=recipient@example.com
DIGEST_EMAIL_FROM=sender@example.com
DIGEST_SMTP_HOST=smtp.example.com
DIGEST_SMTP_PORT=587
DIGEST_SMTP_USERNAME=sender@example.com
DIGEST_SMTP_PASSWORD=<secret>
DIGEST_SMTP_STARTTLS=true
DIGEST_APP_URL=https://web-production-d42333.up.railway.app
```

Optional tuning variables:

```text
DIGEST_MIN_SCORE=75
DIGEST_MIN_CONFIDENCE=80
DIGEST_LOOKBACK_HOURS=24
DIGEST_MAX_COMPANIES=20
DIGEST_TIMEZONE=America/New_York
```

For SMTP over implicit TLS (typically port 465), use
`DIGEST_SMTP_SSL=true`; this disables STARTTLS by default.
