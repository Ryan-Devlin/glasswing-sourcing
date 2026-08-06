"""Send a low-noise daily email of newly qualified Glasswing companies.

The command is deliberately a run-to-completion job so Railway can schedule it
as a cron service without paying for another always-on worker. Delivery history
lives in the shared classification database, making retries idempotent across
containers and deployments.
"""

from __future__ import annotations

import argparse
import asyncio
import html
import json
import os
import smtplib
import ssl
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
import urllib.error
import urllib.request
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from classification_store import (
    CLASSIFICATION_DB,
    classification_source_label,
    connect_store,
    upsert_claude_diligence,
)
from common import clean, get_model
from db_backend import begin_immediate
from diligence_logic import DILIGENCE_VERSION


DEFAULT_MIN_SCORE = 75
DEFAULT_MIN_CONFIDENCE = 80
DEFAULT_MIN_CLAUDE_SCORE = 70
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_MAX_COMPANIES = 5
DEFAULT_CLAUDE_MAX_CALLS = 5


def _integer_env(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)).strip())
    except ValueError:
        value = default
    return max(minimum, min(value, maximum))


def _boolean_env(name: str, default: bool) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _recipients(value: str) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            part.strip().lower()
            for part in value.replace(";", ",").split(",")
            if "@" in part.strip()
        )
    )


@dataclass(frozen=True)
class DigestSettings:
    recipients: tuple[str, ...]
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    sender: str
    use_starttls: bool
    use_ssl: bool
    min_score: int
    min_confidence: int
    min_claude_score: int
    lookback_hours: int
    max_companies: int
    auto_claude: bool
    claude_max_calls: int
    app_url: str
    timezone_name: str

    @classmethod
    def from_environment(cls) -> "DigestSettings":
        username = os.getenv("DIGEST_SMTP_USERNAME", "").strip()
        use_ssl = _boolean_env("DIGEST_SMTP_SSL", False)
        return cls(
            recipients=_recipients(os.getenv("DIGEST_EMAIL_TO", "")),
            smtp_host=os.getenv("DIGEST_SMTP_HOST", "").strip(),
            smtp_port=_integer_env(
                "DIGEST_SMTP_PORT", 465 if use_ssl else 587, 1, 65535
            ),
            smtp_username=username,
            smtp_password=os.getenv("DIGEST_SMTP_PASSWORD", ""),
            sender=os.getenv("DIGEST_EMAIL_FROM", username).strip(),
            use_starttls=_boolean_env("DIGEST_SMTP_STARTTLS", not use_ssl),
            use_ssl=use_ssl,
            min_score=_integer_env(
                "DIGEST_MIN_SCORE", DEFAULT_MIN_SCORE, 0, 100
            ),
            min_confidence=_integer_env(
                "DIGEST_MIN_CONFIDENCE", DEFAULT_MIN_CONFIDENCE, 0, 100
            ),
            min_claude_score=_integer_env(
                "DIGEST_MIN_CLAUDE_SCORE", DEFAULT_MIN_CLAUDE_SCORE, 0, 100
            ),
            lookback_hours=_integer_env(
                "DIGEST_LOOKBACK_HOURS", DEFAULT_LOOKBACK_HOURS, 1, 168
            ),
            max_companies=_integer_env(
                "DIGEST_MAX_COMPANIES", DEFAULT_MAX_COMPANIES, 1, 5
            ),
            auto_claude=_boolean_env("DIGEST_AUTO_CLAUDE", True),
            claude_max_calls=_integer_env(
                "DIGEST_CLAUDE_MAX_CALLS", DEFAULT_CLAUDE_MAX_CALLS, 0, 5
            ),
            app_url=os.getenv("DIGEST_APP_URL", "").strip().rstrip("/"),
            timezone_name=os.getenv(
                "DIGEST_TIMEZONE", "America/New_York"
            ).strip(),
        )

    def validate_for_delivery(self) -> list[str]:
        missing = []
        if not self.recipients:
            missing.append("DIGEST_EMAIL_TO")
        if not self.smtp_host:
            missing.append("DIGEST_SMTP_HOST")
        if not self.sender:
            missing.append("DIGEST_EMAIL_FROM or DIGEST_SMTP_USERNAME")
        if self.smtp_username and not self.smtp_password:
            missing.append("DIGEST_SMTP_PASSWORD")
        return missing


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_digest_schema(database: Path | str = CLASSIFICATION_DB) -> None:
    # A writable connection runs classification_store.migrate(), including
    # digest tables, under the existing cross-service advisory lock.
    with connect_store(database):
        pass


def qualified_candidates(
    settings: DigestSettings,
    *,
    now: datetime | None = None,
    database: Path | str = CLASSIFICATION_DB,
) -> list[dict]:
    """Return recent, unsent companies that clear the trusted digest gate."""
    current = now or utc_now()
    since = _iso(current - timedelta(hours=settings.lookback_hours))
    recipients = settings.recipients or ("__preview__",)
    recipient_marks = ",".join("?" for _ in recipients)
    params: list[object] = [
        settings.min_score,
        settings.min_confidence,
        settings.min_claude_score,
        DILIGENCE_VERSION,
        since,
        since,
        *recipients,
        settings.max_companies,
    ]
    with connect_store(database, read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT c.domain,c.company_name,c.final_url,c.description,c.excerpt,
                   c.routing_reason,c.fit_score,c.confidence_score,
                   c.priority_score,c.thesis_pillar,c.evidence,
                   c.content_source,c.updated_at,
                   cd.decision AS claude_decision,
                   cd.fit_score AS claude_fit_score,
                   cd.product_summary AS claude_summary,
                   cd.reasoning AS claude_reasoning,
                   cd.concerns AS claude_concerns,
                   cd.diligence_sources AS claude_sources,
                   cd.model AS claude_model
            FROM classifications c
            JOIN claude_diligence cd ON cd.domain=c.domain
            WHERE c.routing_bucket='priority'
              AND c.fit_score>=?
              AND c.confidence_score>=?
              AND cd.decision='KEEP'
              AND cd.fit_score>=?
              AND cd.diligence_version=?
              AND (c.updated_at>=? OR cd.updated_at>=?)
              AND NOT EXISTS (
                  SELECT 1 FROM hidden_domains h WHERE h.domain=c.domain
              )
              AND NOT EXISTS (
                  SELECT 1 FROM training_labels t
                  WHERE t.domain=c.domain AND t.label='no'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM daily_digest_items d
                  WHERE d.domain=c.domain
                    AND d.recipient IN ({recipient_marks})
              )
            ORDER BY cd.fit_score DESC,c.priority_score DESC,
                     c.fit_score DESC,c.updated_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def python_candidates_for_claude(
    settings: DigestSettings,
    *,
    now: datetime | None = None,
    database: Path | str = CLASSIFICATION_DB,
) -> list[dict]:
    """Select the strongest fresh Python results that still need Claude."""
    if not settings.auto_claude or settings.claude_max_calls <= 0:
        return []
    current = now or utc_now()
    since = _iso(current - timedelta(hours=settings.lookback_hours))
    with connect_store(database, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT c.*
            FROM classifications c
            LEFT JOIN claude_diligence cd ON cd.domain=c.domain
            WHERE c.routing_bucket='priority'
              AND c.fit_score>=?
              AND c.confidence_score>=?
              AND c.updated_at>=?
              AND (cd.domain IS NULL OR cd.diligence_version<>?)
              AND NOT EXISTS (
                  SELECT 1 FROM hidden_domains h WHERE h.domain=c.domain
              )
              AND NOT EXISTS (
                  SELECT 1 FROM training_labels t
                  WHERE t.domain=c.domain AND t.label='no'
              )
            ORDER BY c.priority_score DESC,c.fit_score DESC,c.updated_at DESC
            LIMIT ?
            """,
            (
                settings.min_score,
                settings.min_confidence,
                since,
                DILIGENCE_VERSION,
                settings.claude_max_calls,
            ),
        ).fetchall()
    candidates = []
    for result in rows:
        row = dict(result)
        try:
            payload = json.loads(str(row.get("record_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload.update({
            "domain": row["domain"],
            "final_url": row.get("final_url", ""),
            "page_title": row.get("company_name", ""),
            "meta_description": row.get("description", ""),
            "homepage_excerpt": row.get("excerpt", ""),
            "rule_score": row.get("fit_score", 0),
            "diligence_rank_score": row.get("fit_score", 0),
            "rule_thesis_pillar": row.get("thesis_pillar", ""),
            "rule_evidence": row.get("evidence", ""),
        })
        candidates.append(payload)
    return candidates


async def _run_claude_diligence(
    rows: list[dict], *, database: Path | str = CLASSIFICATION_DB
) -> int:
    """Run one-search Claude diligence and persist only completed results."""
    if not rows:
        return 0
    from anthropic import AsyncAnthropic
    from claude_classifier import classify

    model = os.getenv("ANTHROPIC_DILIGENCE_MODEL", get_model()).strip()
    client = AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY", "").strip())
    semaphore = asyncio.Semaphore(min(2, len(rows)))
    raw = await asyncio.gather(
        *(classify(client, semaphore, row, model) for row in rows),
        return_exceptions=True,
    )
    completed = []
    for row, assessment in zip(rows, raw):
        if isinstance(assessment, Exception):
            continue
        completed.append({
            **row,
            **{f"llm_{key}": value for key, value in assessment.items()},
            "llm_model": model,
        })
    upsert_claude_diligence(completed, path=database)
    return len(rows)


def run_pending_claude_gate(
    settings: DigestSettings,
    *,
    now: datetime | None = None,
    database: Path | str = CLASSIFICATION_DB,
) -> int:
    """Diligence at most five fresh companies before selecting the email."""
    if not os.getenv("ANTHROPIC_API_KEY", "").strip():
        return 0
    rows = python_candidates_for_claude(
        settings, now=now, database=database
    )
    if not rows:
        return 0
    # The shared store path is selected from the same environment in the
    # Claude helper. Local tests and production therefore persist identically.
    return asyncio.run(_run_claude_diligence(rows, database=database))


def _blurb(row: dict) -> str:
    return clean(row.get("description")) or clean(row.get("excerpt"))


def _evidence(row: dict) -> str:
    value = clean(row.get("evidence"))
    if len(value) > 360:
        return value[:357].rsplit(" ", 1)[0] + "..."
    return value


def _company_name(row: dict) -> str:
    return clean(row.get("company_name")) or clean(row.get("domain"))


def _website(row: dict) -> str:
    return clean(row.get("final_url")) or f"https://{row['domain']}"


def _app_link(settings: DigestSettings, domain: str) -> str:
    if not settings.app_url:
        return ""
    return f"{settings.app_url}/?q={quote_plus(domain)}"


def render_plain_text(
    rows: list[dict], settings: DigestSettings, *, date_label: str
) -> str:
    lines = [
        f"Glasswing daily sourcing digest — {date_label}",
        "",
        (
            f"{len(rows)} new high-confidence prospect"
            f"{'s' if len(rows) != 1 else ''} cleared the digest gate "
            f"after explicit Claude KEEP approval."
        ),
    ]
    for index, row in enumerate(rows, 1):
        lines.extend([
            "",
            f"{index}. {_company_name(row)} — {row['domain']}",
            (
                f"Score {row['fit_score']} | Confidence "
                f"{row['confidence_score']} | "
                f"{classification_source_label(row.get('content_source', ''))}"
            ),
            (
                f"Claude: KEEP | Claude score {row['claude_fit_score']} | "
                f"{clean(row.get('claude_summary'))}"
            ),
            f"Claude reasoning: {clean(row.get('claude_reasoning'))}",
            f"Category: {clean(row.get('thesis_pillar')).replace('_', ' ').title()}",
            f"Why surfaced: {clean(row.get('routing_reason')).replace('_', ' ')}",
            _blurb(row),
        ])
        if _evidence(row):
            lines.append(f"Evidence: {_evidence(row)}")
        lines.append(f"Website: {_website(row)}")
        if _app_link(settings, row["domain"]):
            lines.append(f"Review: {_app_link(settings, row['domain'])}")
    return "\n".join(line for line in lines if line is not None).strip() + "\n"


def render_html(
    rows: list[dict], settings: DigestSettings, *, date_label: str
) -> str:
    cards = []
    for row in rows:
        name = html.escape(_company_name(row))
        domain = html.escape(clean(row["domain"]))
        website = html.escape(_website(row), quote=True)
        category = html.escape(
            clean(row.get("thesis_pillar")).replace("_", " ").title()
        )
        reason = html.escape(
            clean(row.get("routing_reason")).replace("_", " ")
        )
        description = html.escape(_blurb(row))
        evidence = html.escape(_evidence(row))
        source = html.escape(
            classification_source_label(row.get("content_source", ""))
        )
        claude_summary = html.escape(clean(row.get("claude_summary")))
        claude_reasoning = html.escape(clean(row.get("claude_reasoning")))
        review_link = _app_link(settings, row["domain"])
        review = (
            f' · <a href="{html.escape(review_link, quote=True)}">Review in Glasswing</a>'
            if review_link else ""
        )
        evidence_html = (
            f'<p style="margin:8px 0 0;color:#4b5563"><b>Evidence:</b> {evidence}</p>'
            if evidence else ""
        )
        cards.append(f"""
        <div style="border:1px solid #e5e7eb;border-radius:10px;padding:16px;margin:14px 0">
          <div style="font-size:18px;font-weight:700">{name}</div>
          <div style="color:#6b7280;margin:3px 0 10px">{domain} · {source}</div>
          <div style="font-weight:600">Score {int(row['fit_score'])} · Confidence {int(row['confidence_score'])} · {category}</div>
          <div style="font-weight:700;color:#166534;margin-top:6px">Claude KEEP · Score {int(row['claude_fit_score'])} · {claude_summary}</div>
          <p style="margin:8px 0 0;color:#374151"><b>Claude reasoning:</b> {claude_reasoning}</p>
          <p style="margin:8px 0">{description}</p>
          <p style="margin:8px 0 0;color:#4b5563"><b>Why surfaced:</b> {reason}</p>
          {evidence_html}
          <p style="margin:10px 0 0"><a href="{website}">Visit website</a>{review}</p>
        </div>
        """)
    return f"""<!doctype html>
<html><body style="font-family:Arial,sans-serif;color:#111827;max-width:760px;margin:auto;padding:20px">
  <h1 style="margin-bottom:4px">Glasswing daily sourcing digest</h1>
  <div style="color:#6b7280">{html.escape(date_label)}</div>
  <p><b>{len(rows)}</b> new high-confidence prospect{'s' if len(rows) != 1 else ''}
  received explicit Claude KEEP approval after clearing the Python gate.</p>
  {''.join(cards)}
  <p style="color:#6b7280;font-size:12px">A company appears once per recipient. Hidden and Mark No companies are excluded.</p>
</body></html>"""


def build_message(
    rows: list[dict], settings: DigestSettings, *, date_label: str
) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = (
        f"Glasswing: {len(rows)} new high-confidence compan"
        f"{'ies' if len(rows) != 1 else 'y'} — {date_label}"
    )
    message["From"] = settings.sender
    message["To"] = ", ".join(settings.recipients)
    message.set_content(render_plain_text(rows, settings, date_label=date_label))
    message.add_alternative(
        render_html(rows, settings, date_label=date_label), subtype="html"
    )
    return message


RESEND_SMTP_HOST = "smtp.resend.com"
RESEND_API_URL = "https://api.resend.com/emails"


def send_via_resend_api(
    rows: list[dict], settings: DigestSettings, *, date_label: str
) -> None:
    """Resend's HTTP API, not their SMTP relay.

    Reported failure: TimeoutError: timed out, on the very SMTP connection
    attempt (smtplib.SMTP_SSL(..., timeout=30)) -- consistent with cloud
    platforms (Railway included) commonly blocking outbound SMTP ports
    (465/587) by default to curb spam, regardless of credentials. The API
    is a plain HTTPS POST, unaffected by that class of restriction, and
    reuses the same Resend API key already stored as the SMTP password.
    """
    payload = json.dumps({
        "from": settings.sender,
        "to": list(settings.recipients),
        "subject": (
            f"Glasswing: {len(rows)} new high-confidence compan"
            f"{'ies' if len(rows) != 1 else 'y'} — {date_label}"
        ),
        "text": render_plain_text(rows, settings, date_label=date_label),
        "html": render_html(rows, settings, date_label=date_label),
    }).encode("utf-8")
    request = urllib.request.Request(
        RESEND_API_URL,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {settings.smtp_password}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            # Reported failure: HTTP 403, body "error code: 1010" -- that's
            # Cloudflare's own bot-protection signature, not a Resend error
            # code (Resend's real error codes are named strings, e.g.
            # "validation_error"). urllib's default User-Agent
            # ("Python-urllib/3.x") is exactly the kind of generic scripting
            # signature WAFs commonly block outright, before the request
            # ever reaches Resend's API.
            "User-Agent": "glasswing-sourcing-daily-digest/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Resend API error {exc.code}: {detail}") from exc


SENDGRID_SMTP_HOST = "smtp.sendgrid.net"
SENDGRID_API_URL = "https://api.sendgrid.com/v3/mail/send"


def send_via_sendgrid_api(
    rows: list[dict], settings: DigestSettings, *, date_label: str
) -> None:
    """SendGrid's HTTP API, not their SMTP relay -- same reasoning as
    send_via_resend_api: a plain HTTPS POST isn't affected by cloud
    platforms (Railway included) commonly blocking outbound SMTP ports
    regardless of credentials."""
    payload = json.dumps({
        "personalizations": [{"to": [{"email": r} for r in settings.recipients]}],
        "from": {"email": settings.sender},
        "subject": (
            f"Glasswing: {len(rows)} new high-confidence compan"
            f"{'ies' if len(rows) != 1 else 'y'} — {date_label}"
        ),
        "content": [
            {
                "type": "text/plain",
                "value": render_plain_text(rows, settings, date_label=date_label),
            },
            {
                "type": "text/html",
                "value": render_html(rows, settings, date_label=date_label),
            },
        ],
    }).encode("utf-8")
    request = urllib.request.Request(
        SENDGRID_API_URL,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {settings.smtp_password}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "glasswing-sourcing-daily-digest/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"SendGrid API error {exc.code}: {detail}") from exc


def send_message(message: EmailMessage, settings: DigestSettings) -> None:
    context = ssl.create_default_context()
    if settings.use_ssl:
        client_context = smtplib.SMTP_SSL(
            settings.smtp_host,
            settings.smtp_port,
            timeout=30,
            context=context,
        )
    else:
        client_context = smtplib.SMTP(
            settings.smtp_host,
            settings.smtp_port,
            timeout=30,
        )
    with client_context as client:
        if settings.use_starttls and not settings.use_ssl:
            client.starttls(context=context)
        if settings.smtp_username:
            client.login(settings.smtp_username, settings.smtp_password)
        client.send_message(message)


def record_run(
    run_id: str,
    *,
    status: str,
    started_at: str,
    completed_at: str = "",
    candidate_count: int = 0,
    sent_count: int = 0,
    recipient_count: int = 0,
    error: str = "",
    database: Path | str = CLASSIFICATION_DB,
) -> None:
    with connect_store(database) as conn:
        begin_immediate(conn)
        conn.execute(
            """
            INSERT INTO daily_digest_runs(
                run_id,status,started_at,completed_at,candidate_count,
                sent_count,recipient_count,error
            ) VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(run_id) DO UPDATE SET
                status=excluded.status,
                completed_at=excluded.completed_at,
                candidate_count=excluded.candidate_count,
                sent_count=excluded.sent_count,
                recipient_count=excluded.recipient_count,
                error=excluded.error
            """,
            (
                run_id, status, started_at, completed_at, candidate_count,
                sent_count, recipient_count, error[:1000],
            ),
        )
        conn.commit()


def record_deliveries(
    run_id: str,
    rows: list[dict],
    recipients: tuple[str, ...],
    *,
    sent_at: str,
    database: Path | str = CLASSIFICATION_DB,
) -> None:
    values = [
        (recipient, row["domain"], run_id, sent_at)
        for recipient in recipients
        for row in rows
    ]
    if not values:
        return
    with connect_store(database) as conn:
        begin_immediate(conn)
        conn.executemany(
            """
            INSERT INTO daily_digest_items(recipient,domain,run_id,sent_at)
            VALUES (?,?,?,?)
            ON CONFLICT(recipient,domain) DO NOTHING
            """,
            values,
        )
        conn.commit()


def _date_label(now: datetime, timezone_name: str) -> str:
    try:
        local = now.astimezone(ZoneInfo(timezone_name))
    except ZoneInfoNotFoundError:
        local = now
    return local.strftime("%B %-d, %Y")


def run_digest(
    settings: DigestSettings,
    *,
    dry_run: bool = False,
    now: datetime | None = None,
    database: Path | str = CLASSIFICATION_DB,
) -> tuple[int, str]:
    current = now or utc_now()
    run_id = uuid.uuid4().hex
    started_at = _iso(current)
    ensure_digest_schema(database)
    if not dry_run:
        missing = settings.validate_for_delivery()
        if missing:
            raise RuntimeError("Missing digest configuration: " + ", ".join(missing))
        run_pending_claude_gate(settings, now=current, database=database)
    rows = qualified_candidates(settings, now=current, database=database)
    date_label = _date_label(current, settings.timezone_name)
    preview = render_plain_text(rows, settings, date_label=date_label)
    if dry_run:
        return len(rows), preview
    if not rows:
        record_run(
            run_id,
            status="empty",
            started_at=started_at,
            completed_at=_iso(utc_now()),
            database=database,
        )
        return 0, "No new qualified companies; no email sent."
    record_run(
        run_id,
        status="sending",
        started_at=started_at,
        candidate_count=len(rows),
        recipient_count=len(settings.recipients),
        database=database,
    )
    try:
        if settings.smtp_host == RESEND_SMTP_HOST:
            send_via_resend_api(rows, settings, date_label=date_label)
        elif settings.smtp_host == SENDGRID_SMTP_HOST:
            send_via_sendgrid_api(rows, settings, date_label=date_label)
        else:
            send_message(
                build_message(rows, settings, date_label=date_label), settings
            )
        sent_at = _iso(utc_now())
        record_deliveries(
            run_id, rows, settings.recipients,
            sent_at=sent_at, database=database,
        )
        record_run(
            run_id,
            status="sent",
            started_at=started_at,
            completed_at=sent_at,
            candidate_count=len(rows),
            sent_count=len(rows),
            recipient_count=len(settings.recipients),
            database=database,
        )
    except Exception as exc:
        record_run(
            run_id,
            status="failed",
            started_at=started_at,
            completed_at=_iso(utc_now()),
            candidate_count=len(rows),
            recipient_count=len(settings.recipients),
            error=f"{type(exc).__name__}: {exc}",
            database=database,
        )
        raise
    return len(rows), f"Sent {len(rows)} companies to {len(settings.recipients)} recipient(s)."


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the candidate digest without sending or recording it.",
    )
    args = parser.parse_args(argv)
    settings = DigestSettings.from_environment()
    try:
        _, output = run_digest(settings, dry_run=args.dry_run)
    except Exception as exc:
        print(f"Daily digest failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
