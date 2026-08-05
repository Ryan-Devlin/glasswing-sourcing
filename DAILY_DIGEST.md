# Daily sourcing digest

`daily_digest.py` sends a low-noise email containing newly classified,
high-confidence Investable companies. It is designed to run as a short-lived
Railway cron job and exits after every run.

Default safeguards:

- Investable (`priority`) bucket only.
- Glasswing fit score of 75 or higher and confidence of 80 or higher.
- The strongest five fresh Python candidates are run through Claude diligence.
- Email requires an explicit Claude `KEEP` decision and Claude score of 70+;
  `MAYBE` and `REJECT` are never delivered.
- Only companies updated in the preceding 24 hours.
- Hidden and Mark No companies are excluded.
- A domain is delivered only once to each recipient.
- A maximum of 5 companies per email.
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
ANTHROPIC_API_KEY=<secret>
```

Optional tuning variables:

```text
DIGEST_MIN_SCORE=75
DIGEST_MIN_CONFIDENCE=80
DIGEST_MIN_CLAUDE_SCORE=70
DIGEST_LOOKBACK_HOURS=24
DIGEST_MAX_COMPANIES=5
DIGEST_AUTO_CLAUDE=true
DIGEST_CLAUDE_MAX_CALLS=5
DIGEST_TIMEZONE=America/New_York
```

The Anthropic key is used only for the small pre-email diligence gate. With the
default settings, the cron makes at most five Claude calls per day and emails
only results for which Claude returns `KEEP`.

For SMTP over implicit TLS (typically port 465), use
`DIGEST_SMTP_SSL=true`; this disables STARTTLS by default.
