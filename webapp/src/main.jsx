import React, {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  Bot,
  CheckCircle2,
  Database,
  Download,
  FileSpreadsheet,
  Globe2,
  HeartPulse,
  Home,
  LogIn,
  LogOut,
  MailCheck,
  Pause,
  Play,
  RefreshCw,
  RotateCcw,
  Search,
  Trash2,
  Upload,
  Users,
  Filter,
  XCircle,
} from "lucide-react";
import "./styles.css";
import "./polish.css";
import glasswingLogo from "../../assets/glasswing_logo.svg";
import glasswingMark from "../../assets/glasswing_mark.png";
import { dealAgeLabel, formatCount as fmt, pageCount } from "./domain";

const NAV = [
  ["home", "Home", Home],
  ["founders", "Founder Signals", Users],
  ["certstream", "CertStream Scraper", Activity],
  ["funnel", "Funnel Attrition", Filter],
  ["csv", "Domain Intake", Database],
  ["domains", "Domain Crawler (.com, .app)", Globe2],
  ["health", "System Health", HeartPulse],
  ["claude", "Claude Queue", Bot],
  ["landscape", "Competitive Landscape", FileSpreadsheet],
];
const BUCKET_TABS = [
  ["investable", "Investable"],
  ["review", "Needs Review"],
  ["denied", "Hard Denied"],
  ["yes", "YES"],
  ["no", "NO"],
];
const AdminContext = createContext({ admin: false, requestLogin: () => {} });
const useAdmin = () => useContext(AdminContext);
const inflightGets = new Map();
const responseCache = new Map();
const RESPONSE_CACHE_MS = 60_000;
function apiSort(sort) {
  const names = {
    company_name:
      sort.direction === "asc" ? "Company: A to Z" : "Company: Z to A",
    domain: sort.direction === "asc" ? "Domain: A to Z" : "Domain: Z to A",
    fit_score:
      sort.direction === "asc" ? "Fit: low to high" : "Fit: high to low",
    updated_at:
      sort.direction === "asc" ? "Oldest classified" : "Recently classified",
  };
  return names[sort.key] || "Recently classified";
}
function shortClaudeReason(value) {
  const reason = String(
    value || "No written reason was saved for this decision.",
  )
    .replace(/\s+/g, " ")
    .trim();
  return reason.length > 260 ? `${reason.slice(0, 257)}…` : reason;
}
function moneyCompact(value) {
  const amount = Number(value || 0);
  if (!amount) return "Undisclosed";
  if (amount >= 1_000_000) return `$${(amount / 1_000_000).toFixed(amount >= 10_000_000 ? 0 : 1)}M`;
  if (amount >= 1_000) return `$${Math.round(amount / 1_000)}K`;
  return `$${amount.toLocaleString()}`;
}
function claudeScorecard(row) {
  const d = row.claude_dimensions || {};
  const dimensions = [
    ["Thesis", d.thesis_fit], ["Enterprise", d.enterprise_value],
    ["Product", d.product_depth], ["Moat", d.technical_moat],
    ["Stage", d.stage_fit], ["Evidence", d.evidence_confidence],
  ].filter(([, value]) => value !== "" && value !== undefined);
  const breakdown = dimensions.length ? ` ${dimensions.map(([name, value]) => `${name} ${value}/5`).join(" · ")}.` : "";
  const unknowns = row.claude_unknowns && row.claude_unknowns !== "none" ? ` Unknowns: ${row.claude_unknowns}.` : "";
  const rationale = row.claude_reasoning || "No written rationale.";
  const concern = row.claude_concerns && !rationale.includes(row.claude_concerns)
    ? ` Concern: ${row.claude_concerns}.` : "";
  return shortClaudeReason(`${rationale}.${concern}${breakdown}${unknowns}`);
}
function ClaudeReviewBadge({ row, prefix = "" }) {
  const score = row.claude_score !== "" && row.claude_score !== undefined
    ? ` · ${row.claude_score}${prefix ? "/100" : ""}` : "";
  return (
    <span className={`pill claude-badge claude-${row.claude_decision.toLowerCase()}`} tabIndex="0">
      {prefix}{row.claude_decision}{score}
      <span className="claude-tooltip" role="tooltip">{claudeScorecard(row)}</span>
    </span>
  );
}
function claudePending(row) {
  return ["queued", "reviewing"].includes(String(row.claude_queue_status || "").toLowerCase());
}
function ClaudePendingBadge({ row }) {
  const reviewing = String(row.claude_queue_status || "").toLowerCase() === "reviewing";
  return (
    <span className="pill claude-pending-badge" title={reviewing ? "Claude diligence is currently running" : "Waiting in the durable Claude diligence queue"}>
      <Bot size={13} /> {reviewing ? "Claude processing" : "Queued for Claude"}
    </span>
  );
}
function platformRecommendation(row) {
  const bucket = String(row.routing_bucket || "").toLowerCase();
  if (bucket === "priority") return ["investable", "Platform · Investable"];
  if (["reject", "hard_denied", "screened_out"].includes(bucket)) return ["denied", "Platform · Denied"];
  return ["review", "Platform · Review"];
}
function DealStatus({ row, workflow = false }) {
  const [platformClass, platformLabel] = platformRecommendation(row);
  const claudeReject = String(row.claude_decision || row.decision || "").toUpperCase() === "REJECT";
  const humanYes = String(row.manual_override_target || row.label || "").toLowerCase() === "yes";
  const stage = String(row.tracking_status || "tracking").toLowerCase();
  const stageLabel = stage === "reached_out" ? "Reached out" : stage === "meeting" ? "Meeting" : "Tracking";
  return (
    <div className="deal-status" aria-label="Deal status">
      {workflow && <span className={`pill workflow-status workflow-${stage}`}>{stageLabel}</span>}
      {!workflow && humanYes && <span className="pill human-yes">Human · YES</span>}
      <span className={`pill platform-status platform-${platformClass}`}>{platformLabel}</span>
      {workflow && claudeReject && <span className="pill human-override">Human override</span>}
    </div>
  );
}
function GreenFlagTags({ row }) {
  const claudeVerifiedYes = String(row.claude_decision || row.decision || "").toUpperCase() === "KEEP";
  const humanYes = String(row.manual_override_target || row.label || "").toLowerCase() === "yes";
  // Tracking is an explicit human override: keep verified positive signals
  // visible there even when Claude's overall decision was REJECT.
  const trackedOverride = row.tracked === true;
  if (!claudeVerifiedYes && !humanYes && !trackedOverride) return null;
  const claudeFlags = Array.isArray(row.claude_green_flags)
    ? row.claude_green_flags
    : Array.isArray(row.green_flags) ? row.green_flags : [];
  const websiteFlags = Array.isArray(row.website_green_flags) ? row.website_green_flags : [];
  const flags = [...claudeFlags, ...websiteFlags].filter((flag, index, all) =>
    index === all.findIndex((candidate) =>
      String(candidate.type || candidate.label) === String(flag.type || flag.label)
    )
  );
  if (!flags.length) return null;
  return (
    <div className="green-flag-tags" aria-label="Positive diligence signals">
      {flags.slice(0, 6).map((flag) => {
        const source = /^https?:\/\//i.test(String(flag.source || "")) ? flag.source : "";
        const Tag = source ? "a" : "span";
        return (
          <Tag
            className="green-flag"
            key={flag.type || flag.label}
            title={flag.evidence || "Verified by diligence"}
            {...(source ? { href: source, target: "_blank", rel: "noreferrer" } : {})}
          >
            {flag.label}
          </Tag>
        );
      })}
    </div>
  );
}
function signalCheckedLabel(value) {
  if (!value) return "Signals not checked";
  const checked = new Date(value);
  if (Number.isNaN(checked.getTime())) return "Signals checked";
  return `Signals checked ${dealAgeLabel(value).toLowerCase()}`;
}
function scorecardTitle(row) {
  const card = row.scorecard && typeof row.scorecard === "object" ? row.scorecard : {};
  const labels = {
    thesis: "Thesis", product: "Product", buyer: "Buyer",
    technical_depth: "Technical", wedge: "Wedge", moat: "Moat", team: "Team",
  };
  const parts = Object.entries(labels)
    .filter(([key]) => Number.isFinite(Number(card[key])))
    .map(([key, label]) => `${label}: ${Number(card[key])}/100`);
  return parts.length ? parts.join(" · ") : "Glasswing evidence score";
}
async function api(path, options = {}) {
  const method = String(options.method || "GET").toUpperCase();
  const request = async () => {
    const attempts = method === "GET" ? 2 : 1;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const controller = new AbortController();
      const timeout = window.setTimeout(() => controller.abort(), method === "GET" ? 15000 : 12000);
      let r;
      try {
        r = await fetch(path, {
          headers: {
            ...(options.body instanceof FormData
              ? {}
              : { "Content-Type": "application/json" }),
            ...(options.headers || {}),
          },
          ...options,
          signal: options.signal || controller.signal,
        });
      } catch (error) {
        if (error.name === "AbortError") throw new Error("The server took too long to save. Please retry.");
        throw error;
      } finally {
        window.clearTimeout(timeout);
      }
      if (r.ok) return r.json();
      const text = await r.text();
      let payload;
      try { payload = JSON.parse(text); } catch {}
      if ([502, 503, 504].includes(r.status) && attempt < attempts - 1) {
        await new Promise((resolve) => setTimeout(resolve, 300));
        continue;
      }
      const error = new Error(payload?.message || text || `Request failed ${r.status}`);
      error.status = r.status;
      error.payload = payload;
      throw error;
    }
    throw new Error("Service temporarily unavailable");
  };
  if (method !== "GET") {
    // A mutation can affect several homepage views. Never hydrate a later
    // navigation from pre-mutation data.
    responseCache.clear();
    return request();
  }
  if (inflightGets.has(path)) return inflightGets.get(path);
  const pending = request()
    .then((data) => {
      responseCache.set(path, { data, savedAt: Date.now() });
      return data;
    })
    .finally(() => inflightGets.delete(path));
  inflightGets.set(path, pending);
  return pending;
}
async function requestClaudeReview(domain, overrideDailyLimit = false) {
  return api(`/api/claude/review/${encodeURIComponent(domain)}`, {
    method: "POST",
    body: JSON.stringify({ override_daily_limit: overrideDailyLimit }),
  });
}
async function waitForClaude(domain, onStatus) {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    const state = await api(`/api/claude/review/${encodeURIComponent(domain)}`);
    onStatus(state.status);
    if (state.status === "completed") return state.result;
    if (state.status === "failed")
      throw new Error(state.error || "Claude review failed");
    await new Promise((resolve) => setTimeout(resolve, 3000));
  }
  throw new Error(
    "Claude review is still queued and will continue in the background",
  );
}
function useApi(path, deps = []) {
  const cached = responseCache.get(path);
  const [state, set] = useState(() => ({
    data:
      cached && Date.now() - cached.savedAt < RESPONSE_CACHE_MS
        ? cached.data
        : null,
    error: "",
    loading: true,
  }));
  useEffect(() => {
    let live = true;
    let retryTimer = null;
    // Refresh in place. Clearing or hiding previously rendered data here makes
    // tall cards/tables collapse to a one-line loader, which changes the page
    // height and forces the browser to move the user's scroll position.
    set((s) => ({ ...s, loading: true, error: "" }));
    const load = () => {
      api(path)
        .then((data) => live && set({ data, error: "", loading: false }))
        .catch(() => {
          if (!live) return;
          // Railway swaps containers during deploys. Keep stale content when
          // available, otherwise show a neutral loader, and retry until the
          // replacement is healthy instead of pinning a permanent 502 card.
          set((current) => ({
            ...current,
            error: "",
            loading: !current.data,
          }));
          retryTimer = window.setTimeout(load, 1000);
        });
    };
    load();
    return () => {
      live = false;
      if (retryTimer) window.clearTimeout(retryTimer);
    };
  }, deps);
  return state;
}
function Metric({ label, value, tone = "" }) {
  return (
    <div className={`metric ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function validateDomainInput(value) {
  const text = String(value || "").trim();
  if (!text) return { domain: "", error: "Enter a domain or website URL." };
  try {
    const parsed = new URL(text.includes("://") ? text : `https://${text}`);
    const host = parsed.hostname.toLowerCase().replace(/^www\./, "").replace(/\.$/, "");
    const labels = host.split(".");
    const validLabels = labels.length >= 2 && labels.every((label) =>
      label && label.length <= 63 && !label.startsWith("-") &&
      !label.endsWith("-") && /^[a-z0-9-]+$/i.test(label)
    );
    const validTld = labels.at(-1)?.length >= 2 && /^(?:[a-z]{2,63}|xn--[a-z0-9-]+)$/i.test(labels.at(-1));
    if (!validLabels || !validTld || /^\d+(?:\.\d+){3}$/.test(host)) {
      throw new Error("invalid host");
    }
    return { domain: host, error: "" };
  } catch {
    return {
      domain: "",
      error: "Enter a complete domain such as company.com or https://company.ai.",
    };
  }
}
function Loading() {
  return (
    <div className="loading">
      <RefreshCw size={18} />
      Loading…
    </div>
  );
}

function DeferredContent({ children, minHeight = 260 }) {
  const [ready, setReady] = useState(false);
  const anchor = useRef(null);
  useEffect(() => {
    if (ready) return undefined;
    if (!("IntersectionObserver" in window)) {
      setReady(true);
      return undefined;
    }
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (!entry.isIntersecting) return;
        setReady(true);
        observer.disconnect();
      },
      { rootMargin: "700px 0px" },
    );
    if (anchor.current) observer.observe(anchor.current);
    return () => observer.disconnect();
  }, [ready]);
  return (
    <div ref={anchor} className="deferred-content" style={{ minHeight }}>
      {ready ? children : null}
    </div>
  );
}
function ErrorBox({ children }) {
  return <div className="error">{children}</div>;
}

function TrackButton({ row, onChanged, className = "card-track" }) {
  const { admin, requestLogin } = useAdmin();
  const [tracked, setTracked] = useState(Boolean(row.tracked));
  const [busy, setBusy] = useState(false);
  useEffect(() => setTracked(Boolean(row.tracked)), [row.domain, row.tracked]);
  return (
    <button
      className={className}
      disabled={tracked || busy}
      title={tracked ? "Already tracking" : "Add to the shared Tracking view"}
      onClick={async () => {
        if (!admin) return requestLogin();
        setBusy(true);
        try {
          await api("/api/tracking", {
            method: "POST",
            body: JSON.stringify({ domain: row.domain }),
          });
          setTracked(true);
          onChanged?.();
          window.dispatchEvent(new CustomEvent("tracking-changed"));
        } finally {
          setBusy(false);
        }
      }}
    >
      {busy ? "Saving…" : tracked ? "Tracking" : "Track"}
    </button>
  );
}
function ApiStatus() {
  const result = useApi("/api/healthz", []);
  const connected = !result.loading && !result.error;
  return (
    <div className={`api ${connected ? "connected" : "disconnected"}`}>
      <span className="dot" />
      {result.loading
        ? "Connecting…"
        : connected
          ? "API connected"
          : "API unavailable"}
    </div>
  );
}
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: "" };
  }
  static getDerivedStateFromError(error) {
    return { error: error?.message || "This page could not load." };
  }
  render() {
    if (this.state.error) {
      return (
        <ErrorBox>
          {this.state.error}{" "}
          <button onClick={() => location.reload()}>Reload</button>
        </ErrorBox>
      );
    }
    return this.props.children;
  }
}
function AdminLogin({ onClose, onLogin }) {
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const submit = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      await api("/api/auth/login", {
        method: "POST",
        body: JSON.stringify({ password }),
      });
      onLogin();
    } catch {
      setError("Incorrect admin password.");
    } finally {
      setBusy(false);
    }
  };
  return (
    <div
      className="modal-bg"
      onMouseDown={(e) => e.target === e.currentTarget && onClose()}
    >
      <form className="modal" onSubmit={submit}>
        <h3>Admin login</h3>
        <p>Admin access enables labels, uploads, and worker controls.</p>
        <label>
          Password
          <input
            autoFocus
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </label>
        {error && <ErrorBox>{error}</ErrorBox>}
        <div className="modal-actions">
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button className="primary" disabled={busy || !password}>
            Log in
          </button>
        </div>
      </form>
    </div>
  );
}
function ClaudeOverrideModal({ domain, onClose, onOverride }) {
  const [busy, setBusy] = useState(false);
  return (
    <div className="modal-bg" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className="modal">
        <h3>Hit daily limit</h3>
        <p>
          Claude has reached today’s configured call limit. Override it for
          <strong> {domain}</strong> and send one additional paid call?
        </p>
        <div className="modal-actions">
          <button disabled={busy} onClick={onClose}>Cancel</button>
          <button className="primary" disabled={busy} onClick={async () => {
            setBusy(true);
            try { await onOverride(); } finally { setBusy(false); }
          }}>
            {busy ? "Sending…" : "Override and send"}
          </button>
        </div>
      </div>
    </div>
  );
}
function Pager({ page, setPage, total, pageSize = 100 }) {
  const max = pageCount(total, pageSize);
  return (
    <div className="pager">
      <button disabled={page <= 1} onClick={() => setPage(page - 1)}>
        ← Previous
      </button>
      <span>
        Page {page.toLocaleString()} of {max.toLocaleString()} · {fmt(total)}{" "}
        records
      </span>
      <button disabled={page >= max} onClick={() => setPage(page + 1)}>
        Next →
      </button>
    </div>
  );
}
function Table({
  rows,
  columns,
  onLabel,
  onReject,
  onClaude,
  claudeBusy,
  sort,
  onSort,
}) {
  if (!rows?.length) return <div className="empty">No records found.</div>;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key}>
                {c.sortable && onSort ? (
                  <button className="sort-button" onClick={() => onSort(c.key)}>
                    {c.label}
                    <span>
                      {sort?.key === c.key
                        ? sort.direction === "asc"
                          ? "↑"
                          : "↓"
                        : "↕"}
                    </span>
                  </button>
                ) : (
                  c.label
                )}
              </th>
            ))}
            {(onLabel || onReject || onClaude) && <th>Actions</th>}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const bucketed = Number(row.manual_override || 0) === 1 || Boolean(row.manual_override_target || row.label);
            const bucketLabel = String(row.manual_override_target || row.label || "").toUpperCase();
            return (
            <tr key={row.domain || i} className={bucketed ? "already-bucketed" : ""}>
              {columns.map((c) => (
                <td key={c.key} className={`cell-${c.key}`}>
                  {c.render ? (
                    c.render(row)
                  ) : [
                      "company_name",
                      "title",
                      "description",
                      "snippet",
                      "thesis_pillar",
                      "Detail",
                      "detail",
                      "error",
                    ].includes(c.key) ? (
                    <span
                      className="clamp-text"
                      title={String(row[c.key] ?? "")}
                    >
                      {String(row[c.key] ?? "")}
                    </span>
                  ) : (
                    String(row[c.key] ?? "")
                  )}
                </td>
              ))}
              {(onLabel || onReject || onClaude) && (
                <td className="actions">
                  {onLabel && (
                    <>
                      <button
                        className="yes"
                        disabled={bucketed}
                        title={bucketed ? `Already bucketed ${bucketLabel}` : "Move to YES"}
                        onClick={() => onLabel(row, "yes")}
                      >
                        Yes
                      </button>
                    </>
                  )}
                  {onReject && (
                    <button
                      className="no"
                      disabled={bucketed}
                      title={bucketed ? `Already bucketed ${bucketLabel}` : "Reject to Hard Denied and NO"}
                      aria-label={`Reject ${row.domain}`}
                      onClick={() => onReject(row)}
                    >
                      Reject
                    </button>
                  )}
                  {onClaude && (
                    <button
                      className="claude"
                      disabled={
                        claudeBusy === row.domain ||
                        Boolean(row.claude_decision) ||
                        claudePending(row)
                      }
                      title={
                        row.claude_decision
                          ? `Reviewed: ${row.claude_decision}`
                          : claudePending(row)
                            ? "Already queued for Claude diligence"
                          : "Send this company through Claude diligence"
                      }
                      onClick={() => onClaude(row)}
                    >
                      <Bot size={13} />
                      {claudeBusy === row.domain
                        ? "Reviewing…"
                        : row.claude_decision
                          ? "Reviewed"
                          : claudePending(row)
                            ? String(row.claude_queue_status).toLowerCase() === "reviewing" ? "Claude processing" : "Queued for Claude"
                          : "Send to Claude"}
                    </button>
                  )}
                </td>
              )}
            </tr>
          )})}
        </tbody>
      </table>
    </div>
  );
}
function Toast({ children }) {
  return children ? (
    <div className="toast">
      <CheckCircle2 size={17} />
      {children}
    </div>
  ) : null;
}
function BucketTabs({ value, onChange, tabs = BUCKET_TABS }) {
  return (
    <div className="bucket-tabs" role="tablist" aria-label="Company buckets">
      {tabs.map(([id, label]) => (
        <button
          key={id}
          role="tab"
          aria-selected={value === id}
          className={value === id ? "active" : ""}
          onClick={() => onChange(id)}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
const companyCols = [
  { key: "company_name", label: "Company", sortable: true },
  {
    key: "domain",
    label: "Domain",
    sortable: true,
    render: (r) => (
      <a href={r.final_url || `https://${r.domain}`} target="_blank">
        {r.domain}
      </a>
    ),
  },
  {
    key: "routing_bucket",
    label: "Bucket",
    render: (r) => (
      <span className={`pill ${r.routing_bucket}`}>
        {String(r.routing_bucket || "").replaceAll("_", " ")}
      </span>
    ),
  },
  { key: "fit_score", label: "Score", sortable: true },
  {
    key: "claude_decision",
    label: "Claude review",
    render: (r) =>
      r.claude_decision ? (
        <ClaudeReviewBadge row={r} />
      ) : (
        <span className="muted">Not reviewed</span>
      ),
  },
  {
    key: "thesis_pillar",
    label: "Category",
    render: (r) => {
      const value = String(r.thesis_pillar || "").replaceAll("_", " ");
      return (
        <span className="clamp-text" title={value}>
          {value}
        </span>
      );
    },
  },
  { key: "description", label: "Description" },
];
function LabelModal({ target, onClose, onSaved }) {
  const [rating, setRating] = useState(target.rating ?? (target.label === "yes" ? 85 : 10));
  const [reason, setReason] = useState("wrong_fit");
  const [note, setNote] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const rejecting = target.label === "no";
  return (
    <div className="modal-bg">
      <div className="modal">
        <h3>
          Mark {target.domain} {target.label.toUpperCase()}
        </h3>
        <label>
          Glasswing score
          <input
            type="number"
            min="0"
            max="100"
            value={rating}
            onChange={(e) => setRating(e.target.value)}
          />
        </label>
        {rejecting && <>
          <label>
            Why are you rejecting it?
            <select value={reason} onChange={(e) => setReason(e.target.value)}>
              <option value="wrong_fit">Wrong thesis fit</option>
              <option value="consumer">Consumer company</option>
              <option value="services_consulting">Services or consulting</option>
              <option value="too_late_stage">Too late-stage</option>
              <option value="portfolio_conflict">Too close to portfolio</option>
              <option value="weak_product">Weak or unclear product</option>
              <option value="wrong_geography">Wrong geography</option>
              <option value="not_a_company">Not a real company</option>
              <option value="other">Other</option>
            </select>
          </label>
          <label>
            Short note <small>optional, one sentence</small>
            <textarea rows="2" maxLength="240" value={note} onChange={(e) => setNote(e.target.value)} placeholder="What specifically made this a no?" />
          </label>
        </>}
        <div className="modal-actions">
          <button onClick={onClose}>Cancel</button>
          <button
            className="primary"
            disabled={saving}
            onClick={async () => {
              setSaving(true);
              setError("");
              try {
                await api("/api/labels", {
                  method: "POST",
                  body: JSON.stringify({
                    domain: target.domain,
                    label: target.label,
                    rating: Number(rating),
                    reason: rejecting ? `manual_reject:${reason}${note.trim() ? ` — ${note.trim()}` : ""}` : "react_review",
                  }),
                });
                onSaved();
              } catch (requestError) {
                setError(requestError.payload?.detail || requestError.message || "Decision could not be saved. Please retry.");
              } finally {
                setSaving(false);
              }
            }}
          >
            Save decision
          </button>
        </div>
        {error && <p className="error inline-notice">{error}</p>}
      </div>
    </div>
  );
}

function RejectButton({ row, onSaved, className = "card-dismiss" }) {
  const { admin, requestLogin } = useAdmin();
  const [target, setTarget] = useState(null);
  return <>
    <button className={className} title="Reject to Hard Denied and NO" onClick={() => {
      if (!admin) return requestLogin();
      setTarget({ domain: row.domain, label: "no", rating: Number(row.fit_score || row.rating || row.score || 0) });
    }}>Reject</button>
    {target && (
      <LabelModal target={target} onClose={() => setTarget(null)} onSaved={() => {
        setTarget(null);
        onSaved?.();
      }}/>
    )}
  </>;
}

function FastReviewQueue() {
  const { admin, requestLogin } = useAdmin();
  const [wave, setWave] = useState(0);
  const [cursor, setCursor] = useState(0);
  const [dismissed, setDismissed] = useState([]);
  const [reason, setReason] = useState("wrong_fit");
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState("");
  const [message, setMessage] = useState("");
  const [keyboardEnabled, setKeyboardEnabled] = useState(false);
  const result = useApi(
    `/api/classifications?bucket=review&scope=history&page=1&page_size=25&sort=Recently%20classified&t=${wave}`,
    [wave],
  );
  const rows = (result.data?.rows || []).filter((row) =>
    !row.hidden && !row.tracked && !row.manual_override && !dismissed.includes(row.domain)
  );
  const activeIndex = rows.length ? Math.min(cursor, rows.length - 1) : 0;
  const row = rows[activeIndex];
  const flash = (value) => {
    setMessage(value);
    setTimeout(() => setMessage(""), 2200);
  };
  const finish = (domain, value) => {
    setDismissed((current) => [...current, domain]);
    setCursor((current) => Math.min(current, Math.max(0, rows.length - 2)));
    setNote("");
    flash(value);
  };
  const decide = async (label) => {
    if (!row || busy) return;
    if (!admin) return requestLogin();
    setBusy(label);
    try {
      await api("/api/labels", {
        method: "POST",
        body: JSON.stringify({
          domain: row.domain,
          label,
          rating: Number(row.fit_score || 0),
          reason: label === "no"
            ? `manual_reject:${reason}${note.trim() ? ` — ${note.trim()}` : ""}`
            : "fast_review_yes",
        }),
      });
      finish(row.domain, `${row.domain} saved to ${label.toUpperCase()}`);
    } catch (error) {
      flash(error.payload?.detail || error.message || "Decision could not be saved");
    } finally {
      setBusy("");
    }
  };
  const track = async () => {
    if (!row || busy) return;
    if (!admin) return requestLogin();
    setBusy("track");
    try {
      await api("/api/tracking", {
        method: "POST",
        body: JSON.stringify({ domain: row.domain }),
      });
      finish(row.domain, `${row.domain} moved to Tracking`);
      window.dispatchEvent(new CustomEvent("tracking-changed"));
    } catch (error) {
      flash(error.payload?.detail || error.message || "Company could not be tracked");
    } finally {
      setBusy("");
    }
  };
  useEffect(() => {
    if (!keyboardEnabled) return undefined;
    const onKeyDown = (event) => {
      const tag = String(event.target?.tagName || "").toLowerCase();
      if (["input", "textarea", "select"].includes(tag) || event.metaKey || event.ctrlKey || event.altKey) return;
      if (event.key.toLowerCase() === "y") { event.preventDefault(); decide("yes"); }
      if (event.key.toLowerCase() === "r") { event.preventDefault(); decide("no"); }
      if (event.key.toLowerCase() === "t") { event.preventDefault(); track(); }
      if (event.key === "ArrowLeft") { event.preventDefault(); setCursor((value) => Math.max(0, value - 1)); }
      if (event.key === "ArrowRight") { event.preventDefault(); setCursor((value) => Math.min(rows.length - 1, value + 1)); }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [row, busy, admin, reason, note, rows.length, keyboardEnabled]);

  return (
    <section className="fast-review">
      <div className="section-title">
        <div>
          <h2>Fast review</h2>
          <p>One deal at a time. Decisions save immediately and advance the queue.</p>
        </div>
        <div className="review-shortcuts" aria-label="Keyboard shortcuts">
          <button
            className={keyboardEnabled ? "keyboard-toggle active" : "keyboard-toggle"}
            aria-pressed={keyboardEnabled}
            onClick={() => setKeyboardEnabled((value) => !value)}
          >
            Keyboard {keyboardEnabled ? "on" : "off"}
          </button>
          {keyboardEnabled && <>
            <span><kbd>Y</kbd> YES</span><span><kbd>R</kbd> Reject</span>
            <span><kbd>T</kbd> Track</span><span><kbd>←</kbd><kbd>→</kbd> Browse</span>
          </>}
        </div>
      </div>
      {message && <p className="notice inline-notice">{message}</p>}
      {result.loading && !result.data ? <Loading /> : result.error && !result.data ? (
        <ErrorBox>{result.error}</ErrorBox>
      ) : !row ? (
        <div className="empty fast-review-empty">
          <strong>Review wave complete.</strong>
          <button onClick={() => { setDismissed([]); setCursor(0); setWave((value) => value + 1); }}>Load next deals</button>
        </div>
      ) : (
        <div className="review-focus-card">
          <div className="review-focus-main">
            <div className="top-pick-head">
              <span className="score-ring" title={scorecardTitle(row)}>{row.fit_score}<small>/100</small></span>
              <span className="pill">{activeIndex + 1} of {rows.length}</span>
            </div>
            <h3>{row.company_name || row.domain}</h3>
            <a href={row.final_url || `https://${row.domain}`} target="_blank" rel="noreferrer">{row.domain}</a>
            <div className="review-focus-meta">
              <span>{String(row.thesis_pillar || "Uncategorized").replaceAll("_", " ")}</span>
              {row.claude_decision && <ClaudeReviewBadge row={row} prefix="Claude " />}
            </div>
            <p>{row.description || "No useful description was retained."}</p>
            <GreenFlagTags row={row} />
          </div>
          <div className="review-focus-controls">
            <label>
              Rejection reason
              <select value={reason} onChange={(event) => setReason(event.target.value)}>
                <option value="wrong_fit">Wrong thesis fit</option>
                <option value="crowded_market">Crowded market</option>
                <option value="weak_product">Weak or unclear product</option>
                <option value="services_consulting">Services or consulting</option>
                <option value="consumer">Consumer company</option>
                <option value="too_late_stage">Too late-stage</option>
                <option value="portfolio_conflict">Too close to portfolio</option>
                <option value="wrong_geography">Wrong geography</option>
                <option value="not_a_company">Not a real company</option>
                <option value="other">Other</option>
              </select>
            </label>
            <label>
              Optional note
              <input value={note} maxLength="240" onChange={(event) => setNote(event.target.value)} placeholder="One useful sentence for calibration" />
            </label>
            <div className="review-focus-actions">
              <button className="yes" disabled={Boolean(busy)} onClick={() => decide("yes")}><kbd>Y</kbd> YES</button>
              <button className="track-review" disabled={Boolean(busy)} onClick={track}><kbd>T</kbd> Track</button>
              <button className="no" disabled={Boolean(busy)} onClick={() => decide("no")}><kbd>R</kbd> Reject</button>
            </div>
            <div className="review-browse-actions">
              <button disabled={activeIndex === 0} onClick={() => setCursor((value) => Math.max(0, value - 1))}>← Previous</button>
              <button disabled={activeIndex >= rows.length - 1} onClick={() => setCursor((value) => Math.min(rows.length - 1, value + 1))}>Skip →</button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}

function HomePage() {
  const overview = useApi("/api/overview", []);
  const d = overview.data;
  return (
    <>
      <header>
        <p className="eyebrow">Glasswing Ventures</p>
        <h1>Sourcing command center</h1>
        <p>Prioritize, track, and advance the strongest new deal flow.</p>
      </header>
      <TrackingDeals />
      <FastReviewQueue />
      <GoodDeals />
      <section>
        <div className="section-title">
          <h2>System overview</h2>
          {d && (
            <span className={`status ${String(d.overall).toLowerCase()}`}>
              {d.overall}
            </span>
          )}
        </div>
        {overview.error && !overview.data ? (
          <ErrorBox>{overview.error}</ErrorBox>
        ) : !d ? (
          <Loading />
        ) : (
          <div className="metrics">
            <Metric
              label="Companies tracked"
              value={fmt(d.counts.classifications)}
            />
            <Metric label="Investable" value={fmt(d.buckets.investable)} />
            <Metric label="Needs review" value={fmt(d.buckets.review)} />
            <Metric label="Hard denied" value={fmt(d.buckets.denied)} />
          </div>
        )}
      </section>
      <DeferredContent minHeight={420}>
        <CompaniesTable showRunScope={false} />
      </DeferredContent>
    </>
  );
}

function GoodDeals() {
  const { admin, requestLogin } = useAdmin();
  const [refresh, setRefresh] = useState(0);
  const [claudeOverride, setClaudeOverride] = useState(null);
  const [claudeBusy, setClaudeBusy] = useState("");
  const [message, setMessage] = useState("");
  useEffect(() => {
    const refreshDeals = () => setRefresh((value) => value + 1);
    window.addEventListener("tracking-changed", refreshDeals);
    return () => window.removeEventListener("tracking-changed", refreshDeals);
  }, []);
  const deals = useApi(
    "/api/classifications?bucket=investable&page=1&page_size=25&sort=Recently%20detected",
    [refresh],
  );
  if (deals.loading && !deals.data) return <Loading />;
  if (deals.error && !deals.data) return <ErrorBox>{deals.error}</ErrorBox>;
  const rows = (deals.data?.rows || [])
    .filter((row) => !row.hidden && !row.tracked)
    .slice(0, 8);
  if (!rows.length) return null;
  const sendClaude = async (row, overrideDailyLimit = false) => {
    if (!admin) return requestLogin();
    setClaudeBusy(row.domain);
    try {
      await requestClaudeReview(row.domain, overrideDailyLimit);
      setClaudeOverride(null);
      setMessage(`${row.domain} queued for Claude diligence`);
      setRefresh((value) => value + 1);
    } catch (error) {
      if (error.status === 429) setClaudeOverride(row);
      else setMessage(`Claude could not review ${row.domain}: ${error.message}`);
    } finally {
      setClaudeBusy("");
    }
  };
  return (
    <section className="top-picks">
      <div className="section-title">
        <div>
          <h2>Good deals</h2>
          <p>Strong sourcing candidates ranked by the Glasswing screen, independent of Claude review status.</p>
        </div>
      </div>
      {message && <p className="notice">{message}</p>}
      <div className="top-pick-grid">
        {rows.map((row) => (
          <article className="top-pick" key={row.domain}>
            <RejectButton row={row} onSaved={() => setRefresh((value) => value + 1)} />
            <TrackButton row={row} />
            <span className="deal-age" title={row.classified_at || row.updated_at}>{dealAgeLabel(row.classified_at || row.updated_at)}</span>
            <div className="top-pick-head">
              <span className="score-ring" title={scorecardTitle(row)}>{row.fit_score}<small>/100</small></span>
              <DealStatus row={row} />
            </div>
            <h3>{row.company_name || row.domain}</h3>
            <a href={row.final_url || `https://${row.domain}`} target="_blank">{row.domain}</a>
            {row.claude_decision ? (
              <div className="deal-claude-review">
                <ClaudeReviewBadge row={row} prefix="Claude " />
              </div>
            ) : claudePending(row) ? (
              <div className="deal-claude-review"><ClaudePendingBadge row={row} /></div>
            ) : (
              <div className="deal-claude-review">
                <button
                  className="pill claude-unreviewed claude-send-badge"
                  disabled={claudeBusy === row.domain}
                  onClick={() => sendClaude(row)}
                >
                  <span className="unreviewed-label">
                    {claudeBusy === row.domain ? "Queueing…" : "Not reviewed by Claude"}
                  </span>
                  <span className="send-label">Send to Claude</span>
                </button>
              </div>
            )}
            {row.form_d_match && (
              <a className="pill form-d-investable" href={row.form_d_filing_url} target="_blank" rel="noreferrer" title={`Form D filed ${row.form_d_filing_date} · ${moneyCompact(row.form_d_offering_amount)} offering`}>
                Form D · {moneyCompact(row.form_d_offering_amount)}
              </a>
            )}
            <GreenFlagTags row={row} />
            <p className="clamp-text" title={row.description || row.claude_summary || row.claude_reasoning || "Company description is being enriched."}>{row.description || row.claude_summary || shortClaudeReason(row.claude_reasoning) || "Company description is being enriched."}</p>
            <span className="top-pick-category">{String(row.thesis_pillar || "enterprise technology").replaceAll("_", " ")}</span>
          </article>
        ))}
      </div>
      {claudeOverride && (
        <ClaudeOverrideModal
          domain={claudeOverride.domain}
          onClose={() => setClaudeOverride(null)}
          onOverride={() => sendClaude(claudeOverride, true)}
        />
      )}
    </section>
  );
}

function TrackingDeals() {
  const { admin, requestLogin } = useAdmin();
  const [refresh, setRefresh] = useState(0);
  const [trackingView, setTrackingView] = useState("tracking");
  const [signalBusy, setSignalBusy] = useState("");
  useEffect(() => {
    const refreshTracking = () => setRefresh((value) => value + 1);
    window.addEventListener("tracking-changed", refreshTracking);
    return () => window.removeEventListener("tracking-changed", refreshTracking);
  }, []);
  const result = useApi("/api/tracking?limit=100", [refresh]);
  if (result.loading && !result.data) return <Loading />;
  if (result.error && !result.data) return <ErrorBox>{result.error}</ErrorBox>;
  const allRows = result.data?.rows || [];
  const activeCount = allRows.filter((row) => (row.tracking_status || "tracking") === "tracking").length;
  const reachedOutCount = allRows.filter((row) => row.tracking_status === "reached_out").length;
  const meetingCount = allRows.filter((row) => row.tracking_status === "meeting").length;
  const rows = allRows
    .filter((row) => (row.tracking_status || "tracking") === trackingView)
    .slice(0, 8);
  const refreshTracking = () => {
    setRefresh((value) => value + 1);
    window.dispatchEvent(new CustomEvent("tracking-changed"));
  };
  const refreshSignals = async (domain = "") => {
    if (!admin) return requestLogin();
    setSignalBusy(domain || "all");
    try {
      await api(
        domain
          ? `/api/green-flags/${encodeURIComponent(domain)}/refresh`
          : "/api/tracking/green-flags/refresh",
        { method: "POST" },
      );
      refreshTracking();
    } finally {
      setSignalBusy("");
    }
  };
  return (
    <section className="top-picks tracking-view">
      <div className="section-title">
        <div>
          <h2>Tracking</h2>
          <p>Companies the team wants to follow, saved across sessions.</p>
        </div>
        <div className="tracking-header-actions">
          <span className="pill">{fmt(result.data?.total || 0)} tracked</span>
          <button
            className="signal-refresh-all"
            disabled={Boolean(signalBusy)}
            onClick={() => refreshSignals()}
            title="Recheck retained evidence for every tracked company"
          >
            <RefreshCw className={signalBusy === "all" ? "spin" : ""} size={13} />
            {signalBusy === "all" ? "Checking…" : "Refresh all signals"}
          </button>
        </div>
      </div>
      <BucketTabs
        value={trackingView}
        onChange={setTrackingView}
        tabs={[
          ["tracking", `Tracking · ${activeCount}`],
          ["reached_out", `Reached out · ${reachedOutCount}`],
          ["meeting", `Meeting · ${meetingCount}`],
        ]}
      />
      {!rows.length ? (
        <p className="empty-tracking">
          {trackingView === "tracking"
            ? "Click Track on a good deal to add it here."
            : trackingView === "reached_out"
              ? "Companies marked Reached out will appear here."
              : "Companies with an active meeting will appear here."}
        </p>
      ) : (
        <div className="top-pick-grid">
          {rows.map((row) => (
            <article className="top-pick" key={row.domain}>
              <div className="tracking-card-actions">
                <div className="tracking-stage-actions"><button
                  className="tracking-stage"
                  title={trackingView === "tracking" ? "Mark as reached out" : "Move back one stage"}
                  onClick={async () => {
                    if (!admin) return requestLogin();
                    const status = trackingView === "meeting" ? "reached_out" : trackingView === "reached_out" ? "tracking" : "reached_out";
                    await api(`/api/tracking/${encodeURIComponent(row.domain)}`, {
                      method: "PATCH",
                      body: JSON.stringify({ status }),
                    });
                    refreshTracking();
                  }}
                >
                  {trackingView === "tracking" ? <MailCheck size={14} /> : <RotateCcw size={14} />}
                  {trackingView === "tracking" ? "Reached out" : "Move back"}
                </button>
                {trackingView === "reached_out" && <button className="tracking-stage" title="Move to Meeting" onClick={async () => {
                  if (!admin) return requestLogin();
                  await api(`/api/tracking/${encodeURIComponent(row.domain)}`, { method: "PATCH", body: JSON.stringify({ status: "meeting" }) });
                  refreshTracking();
                }}><MailCheck size={14}/> Meeting</button>}
                {trackingView === "meeting" && <button className="tracking-stage" title="Build a sourced competitive landscape" onClick={() => {
                  sessionStorage.setItem("landscape_prefill", row.domain);
                  location.hash = "landscape";
                }}><FileSpreadsheet size={14}/> Build landscape</button>}</div>
                <div className="tracking-secondary-actions"><button
                  className="tracking-untrack"
                  title="Remove from Tracking"
                  onClick={async () => {
                    if (!admin) return requestLogin();
                    await api(`/api/tracking/${encodeURIComponent(row.domain)}`, { method: "DELETE" });
                    refreshTracking();
                  }}
                >
                  Untrack
                </button>
                <button
                  className="tracking-kill"
                  title="Add domain and score to YES, then remove this card"
                  onClick={async () => {
                    if (!admin) return requestLogin();
                    if (!window.confirm(`Kill ${row.domain}? This adds it to YES and removes it from active review views.`)) return;
                    await api(`/api/tracking/${encodeURIComponent(row.domain)}/kill`, { method: "POST" });
                    refreshTracking();
                  }}
                >
                  <XCircle size={14} /> Kill
                </button></div>
              </div>
              <span className="deal-age" title={row.classified_at || row.updated_at}>
                {dealAgeLabel(row.classified_at || row.updated_at)}
              </span>
              <div className="top-pick-head">
                <span className="score-ring" title={scorecardTitle(row)}>{row.fit_score}<small>/100</small></span>
                <DealStatus row={row} workflow />
              </div>
              <h3>{row.company_name || row.domain}</h3>
              <a href={row.final_url || `https://${row.domain}`} target="_blank">{row.domain}</a>
              {row.claude_decision && (
                <div className="deal-claude-review"><ClaudeReviewBadge row={row} prefix="Claude " /></div>
              )}
              <GreenFlagTags row={row} />
              <div className="signal-check-row">
                <span title={row.green_flags_checked_at || ""}>{signalCheckedLabel(row.green_flags_checked_at)}</span>
                <button
                  disabled={Boolean(signalBusy)}
                  onClick={() => refreshSignals(row.domain)}
                  title="Recheck this company's retained evidence"
                >
                  <RefreshCw className={signalBusy === row.domain ? "spin" : ""} size={11} />
                  {signalBusy === row.domain ? "Checking…" : "Refresh"}
                </button>
              </div>
              <p className="clamp-text" title={row.description || row.claude_summary || row.claude_reasoning || "Company description is being enriched."}>{row.description || row.claude_summary || shortClaudeReason(row.claude_reasoning) || "Company description is being enriched."}</p>
              <span className="top-pick-category">{String(row.thesis_pillar || "enterprise technology").replaceAll("_", " ")}</span>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function ClaudeTopPicks() {
  const { admin, requestLogin } = useAdmin();
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    const refreshPicks = () => setRefresh((value) => value + 1);
    window.addEventListener("tracking-changed", refreshPicks);
    return () => window.removeEventListener("tracking-changed", refreshPicks);
  }, []);
  const picks = useApi("/api/claude/top?limit=20", [refresh]);
  if (picks.loading && !picks.data) return <Loading />;
  if ((picks.error && !picks.data) || !picks.data?.rows?.length) return null;
  const visiblePicks = picks.data.rows
    .filter((row) => !row.tracked)
    .slice(0, 8);
  if (!visiblePicks.length) return null;
  return (
    <section className="top-picks">
      <div className="section-title">
        <div>
          <p className="eyebrow">Highest conviction</p>
          <h2>Claude’s strongest investment recommendations</h2>
          <p>Decisive KEEP reviews, ranked by Claude’s diligence score.</p>
        </div>
        <a className="button-link" href="#claude">
          View Claude queue
        </a>
      </div>
      <div className="top-pick-grid">
        {visiblePicks.map((row) => {
          const reason = shortClaudeReason(
            row.reasoning || row.product_summary,
          );
          return (
            <article className="top-pick" key={row.domain}>
              <span className="deal-age" title={row.classified_at || row.updated_at}>{dealAgeLabel(row.classified_at || row.updated_at)}</span>
              <RejectButton row={row} onSaved={() => setRefresh((value) => value + 1)} />
              <TrackButton row={row} />
              <div className="top-pick-head">
                <span className="score-ring">
                  {row.fit_score}
                  <small>/100</small>
                </span>
                <DealStatus row={row} />
              </div>
              <h3>{row.company_name || row.domain}</h3>
              <a
                href={row.final_url || `https://${row.domain}`}
                target="_blank"
              >
                {row.domain}
              </a>
              <div className="deal-claude-review">
                <ClaudeReviewBadge row={row} prefix="Claude " />
              </div>
              <GreenFlagTags row={row} />
              <p className="clamp-text" title={reason}>
                {reason}
              </p>
              {row.thesis_fit !== undefined && (
                <div className="dimension-row" title={`Evidence confidence ${row.evidence_confidence}/5`}>
                  <span>Thesis {row.thesis_fit}/5</span>
                  <span>Enterprise {row.enterprise_value}/5</span>
                  <span>Product {row.product_depth}/5</span>
                  <span>Moat {row.technical_moat}/5</span>
                </div>
              )}
              {row.thesis_pillar && (
                <span className="top-pick-category">
                  {String(row.thesis_pillar).replaceAll("_", " ")}
                </span>
              )}
            </article>
          );
        })}
      </div>
    </section>
  );
}

function LabelsPanel() {
  const { admin, requestLogin } = useAdmin();
  const [label, setLabel] = useState("yes");
  const [refresh, setRefresh] = useState(0);
  const [page, setPage] = useState(1);
  const result = useApi(
    `/api/labels?label=${label}&page=${page}&page_size=100`,
    [label, page, refresh],
  );
  const remove = async (domain) => {
    if (
      !window.confirm(
        `Remove ${domain} from the ${label.toUpperCase()} basket?`,
      )
    )
      return;
    await api(`/api/labels/${encodeURIComponent(domain)}`, {
      method: "DELETE",
    });
    setRefresh((value) => value + 1);
  };
  return (
    <section>
      <div className="table-toolbar">
        <div>
          <h2>Training baskets</h2>
          <p>
            Explicit company decisions shared across every worker and reviewer.
          </p>
        </div>
        <BucketTabs
          value={label}
          onChange={(value) => {
            setLabel(value);
            setPage(1);
          }}
          tabs={[
            ["yes", "YES"],
            ["no", "NO"],
          ]}
        />
      </div>
      <div className="inline-tools">
        <a className="button-link" href={`/api/export/labels/${label}`}>
          Export CSV
        </a>
      </div>
      {result.loading && !result.data ? (
        <Loading />
      ) : result.error && !result.data ? (
        <ErrorBox>{result.error}</ErrorBox>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Domain</th>
                <th>Glasswing score</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {result.data.rows.map((row) => (
                <tr key={row.domain}>
                  <td>
                    <a href={row.url} target="_blank">
                      {row.domain}
                    </a>
                  </td>
                  <td>{row.rating}</td>
                  <td>
                    <button
                      className="no"
                      onClick={() =>
                        admin ? remove(row.domain) : requestLogin()
                      }
                    >
                      <Trash2 size={13} />
                      {admin ? "Remove" : "Admin"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {result.data && (
        <Pager page={page} setPage={setPage} total={result.data.total} />
      )}
    </section>
  );
}

function CompaniesTable({ showRunScope = false }) {
  const { admin, requestLogin } = useAdmin();
  const [bucket, setBucket] = useState("investable");
  const [scope, setScope] = useState(showRunScope ? "current" : "history");
  const [page, setPage] = useState(1);
  const [refresh, setRefresh] = useState(0);
  const [label, setLabel] = useState(null);
  const [toast, setToast] = useState("");
  const [sort, setSort] = useState({ key: "updated_at", direction: "desc" });
  const [claudeBusy, setClaudeBusy] = useState("");
  const [claudeOverride, setClaudeOverride] = useState(null);
  const basket = bucket === "yes" || bucket === "no";
  const query = basket
    ? `/api/labels?label=${bucket}&page=${page}&page_size=100`
    : `/api/classifications?bucket=${bucket}&scope=${scope}&page=${page}&page_size=100&sort=${encodeURIComponent(apiSort(sort))}`;
  const result = useApi(query, [
    bucket,
    scope,
    page,
    refresh,
    sort.key,
    sort.direction,
  ]);
  const save = (row, label) => setLabel({ domain: row.domain, label });
  const reject = (row) => setLabel({
    domain: row.domain, label: "no", rating: Number(row.fit_score || row.rating || 0),
  });
  const sendClaude = async (row, overrideDailyLimit = false) => {
    if (!admin) return requestLogin();
    setClaudeBusy(row.domain);
    try {
      const response = await requestClaudeReview(row.domain, overrideDailyLimit);
      if (overrideDailyLimit) setClaudeOverride(null);
      if (response.status === "already_reviewed") {
        setToast(`${row.domain} was already reviewed by Claude`);
      } else {
        setToast(`${row.domain} queued for Claude`);
        const decision = await waitForClaude(row.domain, (status) =>
          setToast(
            `${row.domain}: ${status === "reviewing" ? "Claude is reviewing…" : "queued for Claude"}`,
          ),
        );
        setToast(
          `${row.domain}: Claude ${decision.decision} · score ${decision.fit_score}`,
        );
      }
      setRefresh((value) => value + 1);
    } catch (error) {
      if (error.status === 429) {
        setClaudeOverride(row);
      } else {
        setToast(`Claude could not review ${row.domain}: ${error.message}`);
      }
    } finally {
      setClaudeBusy("");
      setTimeout(() => setToast(""), 5000);
    }
  };
  return (
    <section>
      <Toast>{toast}</Toast>
      <div className="table-toolbar table-heading">
        <div>
          <h2>Classification results</h2>
          <p>
            Server-paginated. Changing pages never downloads the complete
            corpus.
          </p>
        </div>
      </div>
      {showRunScope && !basket && (
        <div className="segmented scope-tabs" aria-label="Classification history scope">
          <button
            className={scope === "current" ? "active" : ""}
            onClick={() => { setScope("current"); setPage(1); }}
          >
            Current run
          </button>
          <button
            className={scope === "history" ? "active" : ""}
            onClick={() => { setScope("history"); setPage(1); }}
          >
            All history
          </button>
        </div>
      )}
      <BucketTabs
        value={bucket}
        onChange={(value) => {
          setBucket(value);
          setPage(1);
        }}
      />
      <div className="export-row">
        <a
          className="button-link"
          href={basket
            ? `/api/export/labels/${bucket}`
            : `/api/export/classifications/${bucket}?scope=${scope}`}
        >
          Export current bucket CSV
        </a>
      </div>
      {result.loading && !result.data ? (
        <Loading />
      ) : result.error && !result.data ? (
        <ErrorBox>{result.error}</ErrorBox>
      ) : basket ? (
        <>
          <Table
            rows={result.data.rows.filter((row) => !row.hidden)}
            columns={[
              {
                key: "domain",
                label: "Domain",
                render: (row) => (
                  <a href={row.url} target="_blank">
                    {row.domain}
                  </a>
                ),
              },
              { key: "rating", label: "Glasswing score" },
            ]}
            onReject={admin ? reject : requestLogin}
          />
          <Pager page={page} setPage={setPage} total={result.data.total} />
        </>
      ) : (
        <>
          <Table
            rows={result.data.rows.filter((r) => !r.hidden)}
            columns={companyCols}
            onLabel={admin ? save : (_, label) => requestLogin(label)}
            onReject={admin ? reject : requestLogin}
            onClaude={admin ? sendClaude : requestLogin}
            claudeBusy={claudeBusy}
            sort={sort}
            onSort={(key) => {
              setSort((current) => ({
                key,
                direction:
                  current.key === key && current.direction === "desc"
                    ? "asc"
                    : "desc",
              }));
              setPage(1);
            }}
          />
          <Pager page={page} setPage={setPage} total={result.data.total} />
        </>
      )}
      {label && (
        <LabelModal
          target={label}
          onClose={() => setLabel(null)}
          onSaved={() => {
            setToast(`${label.domain} saved to ${label.label.toUpperCase()}`);
            setLabel(null);
            setRefresh((x) => x + 1);
            setTimeout(() => setToast(""), 2500);
          }}
        />
      )}
      {claudeOverride && (
        <ClaudeOverrideModal
          domain={claudeOverride.domain}
          onClose={() => setClaudeOverride(null)}
          onOverride={() => sendClaude(claudeOverride, true)}
        />
      )}
    </section>
  );
}
function RunFilters({ running }) {
  const { admin, requestLogin } = useAdmin();
  const result = useApi("/api/pipeline/filters", []);
  const [form, setForm] = useState(null);
  const [message, setMessage] = useState("");
  const edited = useRef(false);
  const updateForm = (next) => {
    edited.current = true;
    setForm(next);
  };
  useEffect(() => {
    // Seed from the server, but never overwrite edits in progress. useApi
    // hands back a new object on every fetch (and retries once a second while
    // the API is briefly unreachable during a deploy), so keying this on
    // result.data used to snap a half-changed extension list back to the saved
    // value mid-edit.
    if (result.data && !edited.current) setForm(result.data);
  }, [result.data]);
  if (!form) {
    return result.error ? <ErrorBox>{result.error}</ErrorBox> : <Loading />;
  }
  const extensions = [
    ".ai",
    ".app",
    ".dev",
    ".io",
    ".co",
    ".xyz",
    ".tech",
    ".com",
  ];
  const save = async () => {
    if (!admin) return requestLogin();
    if (running) return;
    const saved = await api("/api/pipeline/filters", {
      method: "POST",
      body: JSON.stringify(form),
    });
    edited.current = false;
    setForm(saved);
    setMessage(
      "Filters saved. The pipeline will use them for incoming domains.",
    );
  };
  return (
    <section className="filter-panel">
      <div className="section-title">
        <div>
          <h2>Run filters</h2>
          <p>Control which certificate domains enter screening.</p>
        </div>
      </div>
      {running && (
        <p className="notice">
          Pause CertStream to unlock and change these filters.
        </p>
      )}
      <div className="extension-grid">
        {extensions.map((extension) => (
          <label key={extension}>
            <input
              type="checkbox"
              disabled={running}
              checked={form.selected_extensions.includes(extension)}
              onChange={(event) => {
                updateForm({
                  ...form,
                  selected_extensions: event.target.checked
                    ? [...form.selected_extensions, extension]
                    : form.selected_extensions.filter(
                        (value) => value !== extension,
                      ),
                });
              }}
            />
            {extension}
          </label>
        ))}
      </div>
      <div className="filter-fields">
        <label>
          Include keywords
          <input
            disabled={running}
            value={form.include_keywords.join(", ")}
            placeholder="robotics, security, infrastructure"
            onChange={(e) =>
              updateForm({
                ...form,
                include_keywords: e.target.value
                  .split(",")
                  .map((x) => x.trim())
                  .filter(Boolean),
              })
            }
          />
        </label>
        <label>
          Exclude keywords
          <input
            disabled={running}
            value={form.exclude_keywords.join(", ")}
            placeholder="casino, dating, consumer"
            onChange={(e) =>
              updateForm({
                ...form,
                exclude_keywords: e.target.value
                  .split(",")
                  .map((x) => x.trim())
                  .filter(Boolean),
              })
            }
          />
        </label>
        <label>
          Minimum score
          <input
            type="number"
            disabled={running}
            min="0"
            max="100"
            value={form.minimum_score}
            onChange={(e) =>
              updateForm({ ...form, minimum_score: Number(e.target.value) })
            }
          />
        </label>
        <label className="claude-limit-control">
          Claude calls per day
          <div>
            <input
              type="range"
              disabled={running}
              min="1"
              max="1000"
              step="25"
              value={form.claude_daily_limit ?? 400}
              onChange={(e) =>
                updateForm({ ...form, claude_daily_limit: Number(e.target.value) })
              }
            />
            <input
              type="number"
              disabled={running}
              min="1"
              max="5000"
              value={form.claude_daily_limit ?? 400}
              onChange={(e) =>
                updateForm({
                  ...form,
                  claude_daily_limit: Math.max(
                    1,
                    Math.min(5000, Number(e.target.value) || 1),
                  ),
                })
              }
            />
          </div>
          <span>
            Rolling 24-hour paid-call cap. Hourly safety cap remains 100.
          </span>
        </label>
      </div>
      <div className="filter-actions">
        <button className="primary" disabled={running} onClick={save}>
          {running
            ? "Pause to edit filters"
            : admin
              ? "Save filters"
              : "Admin login to edit"}
        </button>
        {message && <span>{message}</span>}
      </div>
    </section>
  );
}
function CertStreamPage() {
  const { admin, requestLogin } = useAdmin();
  const [tick, setTick] = useState(0);
  const [controlBusy, setControlBusy] = useState(false);
  const [requestedState, setRequestedState] = useState(null);
  const [controlMessage, setControlMessage] = useState("");
  const status = useApi("/api/certstream/status", [tick]);
  useEffect(() => {
    const id = setInterval(() => {
      if (document.visibilityState === "visible") setTick((x) => x + 1);
    }, 30000);
    return () => clearInterval(id);
  }, []);
  const s = status.data || {};
  // Before the first response there is no data, and rendering fmt(undefined)
  // showed a confident "0" in every funnel tile that then jumped to the real
  // number -- indistinguishable from the run actually resetting. Show a
  // placeholder until real numbers exist.
  const metric = (value) =>
    status.data === null || value === undefined || value === null
      ? "—"
      : fmt(value);
  const desiredRunning = requestedState ?? s.desired_running ?? s.running;
  useEffect(() => {
    if (s.desired_running === requestedState) setRequestedState(null);
  }, [s.desired_running, requestedState]);
  const control = async (desired) => {
    setRequestedState(desired);
    setControlMessage(desired ? "Starting CertStream…" : "Pause requested…");
    setControlBusy(true);
    try {
      await api("/api/runtime/pipeline", {
        method: "POST",
        body: JSON.stringify({ desired }),
      });
      setControlMessage(
        desired
          ? "CertStream resumed. New domains are being collected."
          : "CertStream paused. Intake is disconnected; no new domains will be collected.",
      );
      setTick((x) => x + 1);
    } catch (error) {
      setRequestedState(null);
      setControlMessage(`Could not change CertStream state: ${error.message}`);
    } finally {
      setControlBusy(false);
    }
  };
  return (
    <>
      <header>
        <p className="eyebrow">Live acquisition</p>
        <h1>CertStream Scraper</h1>
        <p>Live worker state updates independently from the results table.</p>
      </header>
      <div className="section-title">
        <div>
          <h2>{desiredRunning ? (s.running ? "Running" : "Starting") : "Paused"}</h2>
          <p>
            {desiredRunning
              ? "Collecting certificate domains and screening the queue."
              : "Intake and new screening are stopped. An already-started batch may finish safely."}
          </p>
        </div>
        <div>
          <button
            disabled={controlBusy || desiredRunning}
            onClick={() => (admin ? control(true) : requestLogin())}
          >
            <Play size={16} />
            Start
          </button>
          <button
            disabled={controlBusy || !desiredRunning}
            onClick={() =>
              admin
                ? window.confirm("Pause CertStream screening safely?") &&
                  control(false)
                : requestLogin()
            }
          >
            <Pause size={16} />
            Pause
          </button>
          <button className="icon" onClick={() => setTick((x) => x + 1)}>
            <RefreshCw size={17} />
            Refresh
          </button>
        </div>
      </div>
      {controlMessage && <p className="notice">{controlMessage}</p>}
      <div className="metrics">
        {/* Read left to right as a chain. The first tile is layer 0 -- every
            distinct domain the feed showed us -- while the rest are
            post-filter. Without the middle tile the two ends look like one
            population disagreeing by a factor of 500, which is how the
            filter's own work becomes invisible. */}
        <Metric
          label="Seen on the feed"
          value={metric(s.new_domains_received ?? s.domains_received)}
        />
        <Metric
          label="Queued after filtering"
          value={metric(s.queued_this_run ?? s.current_run_queue?.total)}
        />
        <Metric label="Screening complete" value={metric(s.screening_complete_count ?? s.current_run_queue?.complete)} />
        <Metric
          label="Candidates retained"
          value={metric(s.retained_in_buckets)}
        />
        <Metric
          label="Filtered out"
          value={metric(s.discarded_after_screening)}
        />
        {/* Terminal with nothing recorded. It used to be folded into
            "filtered out", which made a crawl that produced no verdict look
            like a judgement and left the tiles refusing to add up. */}
        <Metric label="No verdict" value={metric(s.no_verdict)} />
        <Metric
          label="Still processing"
          value={metric(
            s.still_processing ?? (s.current_run_queue
              ? Number(s.current_run_queue.awaiting_first_attempt || 0) +
                Number(s.current_run_queue.processing || 0) +
                Number(s.current_run_queue.retrying || 0)
              : undefined)
          )}
        />
      </div>
      {s.classification_store_warning && (
        <p className="error inline-notice">{s.classification_store_warning}</p>
      )}
      <RunFilters running={Boolean(desiredRunning)} />
      <CompaniesTable showRunScope />
    </>
  );
}

function CorpusCompanies({ kind }) {
  const { admin, requestLogin } = useAdmin();
  const [bucket, setBucket] = useState("investable");
  const [page, setPage] = useState(1);
  const [refresh, setRefresh] = useState(0);
  const [label, setLabel] = useState(null);
  const [sort, setSort] = useState({ key: "updated_at", direction: "desc" });
  const [claudeBusy, setClaudeBusy] = useState("");
  const [claudeOverride, setClaudeOverride] = useState(null);
  const [toast, setToast] = useState("");
  const reject = (row) => admin
    ? setLabel({ domain: row.domain, label: "no", rating: Number(row.fit_score || row.rating || 0) })
    : requestLogin();
  const basket = bucket === "yes" || bucket === "no";
  const pageSize = 25;
  const result = useApi(
    basket
      ? `/api/labels?label=${bucket}&page=${page}&page_size=${pageSize}`
      : `/api/corpus/${kind}?bucket=${bucket}&page=${page}&page_size=${pageSize}&sort=${encodeURIComponent(apiSort(sort))}`,
    [bucket, page, refresh, sort.key, sort.direction],
  );
  return (
    <section>
      <Toast>{toast}</Toast>
      <div className="table-toolbar table-heading">
        <div>
          <h2>{kind === "zone" ? "Retained companies" : "Crawled company buckets"}</h2>
          <p>{kind === "zone" ? "Only high-scoring review candidates and approved Investable companies are retained; rejected site content is discarded." : "Only records sourced from this crawler are included."}</p>
        </div>
      </div>
      <BucketTabs
        value={bucket}
        tabs={kind === "zone" ? [
          ["investable", "Investable"],
          ["review", "Needs Review"],
          ["yes", "YES"],
          ["no", "NO"],
        ] : BUCKET_TABS}
        onChange={(value) => {
          setBucket(value);
          setPage(1);
        }}
      />
      {result.loading && !result.data ? (
        <Loading />
      ) : result.error && !result.data ? (
        <ErrorBox>{result.error}</ErrorBox>
      ) : basket ? (
        <Table
          rows={result.data.rows}
          columns={[
            {
              key: "domain",
              label: "Domain",
              render: (row) => (
                <a href={row.url} target="_blank">
                  {row.domain}
                </a>
              ),
            },
            { key: "rating", label: "Glasswing score" },
          ]}
          onReject={reject}
        />
      ) : (
        <>
          <Table
            rows={result.data.rows}
            columns={companyCols}
            onLabel={(row, label) =>
              admin ? setLabel({ domain: row.domain, label }) : requestLogin()
            }
            onReject={reject}
            onClaude={async (row) => {
              if (!admin) return requestLogin();
              setClaudeBusy(row.domain);
              try {
                const response = await requestClaudeReview(row.domain);
                if (response.status === "already_reviewed") {
                  setToast(`${row.domain} was already reviewed`);
                } else {
                  setToast(`${row.domain} queued for Claude`);
                  const decision = await waitForClaude(row.domain, (status) =>
                    setToast(
                      `${row.domain}: ${status === "reviewing" ? "Claude is reviewing…" : "queued for Claude"}`,
                    ),
                  );
                  setToast(
                    `${row.domain}: Claude ${decision.decision} · score ${decision.fit_score}`,
                  );
                }
                setRefresh((value) => value + 1);
              } catch (error) {
                if (error.status === 429) setClaudeOverride(row);
                else setToast(
                    `Claude could not review ${row.domain}: ${error.message}`,
                  );
              } finally {
                setClaudeBusy("");
                setTimeout(() => setToast(""), 5000);
              }
            }}
            claudeBusy={claudeBusy}
            sort={sort}
            onSort={(key) => {
              setSort((current) => ({
                key,
                direction:
                  current.key === key && current.direction === "desc"
                    ? "asc"
                    : "desc",
              }));
              setPage(1);
            }}
          />
          <Pager page={page} setPage={setPage} total={result.data.total} pageSize={pageSize} />
        </>
      )}
      {label && (
        <LabelModal
          target={label}
          onClose={() => setLabel(null)}
          onSaved={() => {
            setLabel(null);
            setRefresh((x) => x + 1);
          }}
        />
      )}
      {claudeOverride && (
        <ClaudeOverrideModal
          domain={claudeOverride.domain}
          onClose={() => setClaudeOverride(null)}
          onOverride={async () => {
            setClaudeBusy(claudeOverride.domain);
            try {
              await requestClaudeReview(claudeOverride.domain, true);
              setToast(`${claudeOverride.domain} queued with daily-limit override`);
              setClaudeOverride(null);
              setRefresh((value) => value + 1);
            } catch (error) {
              setToast(`Claude override failed: ${error.message}`);
            } finally {
              setClaudeBusy("");
            }
          }}
        />
      )}
    </section>
  );
}

function CrawlerDiscoveries() {
  const { admin, requestLogin } = useAdmin();
  const [refresh, setRefresh] = useState(0);
  const [claudeBusy, setClaudeBusy] = useState("");
  const [claudeOverride, setClaudeOverride] = useState(null);
  const [message, setMessage] = useState("");
  const result = useApi(
    "/api/corpus/zone/candidates?limit=25",
    [refresh],
  );
  if (result.loading && !result.data) return <Loading />;
  if (result.error && !result.data) return <ErrorBox>{result.error}</ErrorBox>;
  const rows = (result.data?.rows || [])
    .filter((row) => !row.hidden && !row.tracked)
    .slice(0, 8);
  const sendClaude = async (row, overrideDailyLimit = false) => {
    if (!admin) return requestLogin();
    setClaudeBusy(row.domain);
    try {
      await requestClaudeReview(row.domain, overrideDailyLimit);
      setClaudeOverride(null);
      setMessage(`${row.domain} queued for Claude diligence`);
      setRefresh((value) => value + 1);
    } catch (error) {
      if (error.status === 429) setClaudeOverride(row);
      else setMessage(`Claude could not review ${row.domain}: ${error.message}`);
    } finally {
      setClaudeBusy("");
    }
  };
  return (
    <section className="top-picks crawler-discoveries">
      <div className="section-title">
        <div>
          <p className="eyebrow">Deal output</p>
          <h2>Qualified new deal candidates</h2>
          <p>High-scoring companies retained for partner or Claude review. Only approved companies become Investable.</p>
        </div>
        <span className="pill">{fmt(result.data?.total || 0)} qualified</span>
      </div>
      {message && <p className="notice">{message}</p>}
      {!rows.length ? (
        <div className="crawler-empty-state">
          <strong>No qualified candidate is waiting.</strong>
          <span>The crawler is screening the ranked frontier; the next qualified company will appear here automatically.</span>
        </div>
      ) : (
        <div className="top-pick-grid">
          {rows.map((row) => (
            <article className="top-pick" key={row.domain}>
              <RejectButton row={row} onSaved={() => setRefresh((value) => value + 1)} />
              <TrackButton row={row} onChanged={() => setRefresh((value) => value + 1)} />
              <span className="deal-age">{dealAgeLabel(row.classified_at || row.updated_at)}</span>
              <div className="top-pick-head">
                <span className="score-ring" title={scorecardTitle(row)}>{row.fit_score}<small>/100</small></span>
                <DealStatus row={row} />
              </div>
              <h3>{row.company_name || row.domain}</h3>
              <a href={row.final_url || `https://${row.domain}`} target="_blank">{row.domain}</a>
              {row.claude_decision ? (
                <div className="deal-claude-review"><ClaudeReviewBadge row={row} prefix="Claude " /></div>
              ) : claudePending(row) ? (
                <div className="deal-claude-review"><ClaudePendingBadge row={row} /></div>
              ) : (
                <div className="deal-claude-review">
                  <button
                    className="pill claude-unreviewed claude-send-badge"
                    disabled={claudeBusy === row.domain}
                    onClick={() => sendClaude(row)}
                  >
                    {claudeBusy === row.domain ? "Queueing…" : "Send to Claude"}
                  </button>
                </div>
              )}
              <GreenFlagTags row={row} />
              <p className="clamp-text" title={row.description || row.claude_summary || row.claude_reasoning || "Company description is being enriched."}>{row.description || row.claude_summary || shortClaudeReason(row.claude_reasoning) || "Company description is being enriched."}</p>
              <span className="top-pick-category">{String(row.thesis_pillar || "enterprise technology").replaceAll("_", " ")}</span>
            </article>
          ))}
        </div>
      )}
      {claudeOverride && (
        <ClaudeOverrideModal
          domain={claudeOverride.domain}
          onClose={() => setClaudeOverride(null)}
          onOverride={() => sendClaude(claudeOverride, true)}
        />
      )}
    </section>
  );
}

function FormDPage() {
  const { admin, requestLogin } = useAdmin();
  const [bucket, setBucket] = useState("investable");
  const [tick, setTick] = useState(0);
  const [busy, setBusy] = useState(false);
  const result = useApi(`/api/form-d?bucket=${bucket}&limit=100`, [bucket, tick]);
  const data = result.data;
  const control = async (desired) => {
    if (!admin) return requestLogin();
    setBusy(true);
    try {
      await api("/api/form-d/control", { method: "POST", body: JSON.stringify({ desired }) });
      setTick((value) => value + 1);
    } finally { setBusy(false); }
  };
  const refresh = async () => {
    if (!admin) return requestLogin();
    setBusy(true);
    try {
      await api("/api/form-d/refresh", { method: "POST" });
      setTick((value) => value + 1);
    } finally { setBusy(false); }
  };
  const money = (value) => value ? `$${Number(value).toLocaleString()}` : "Not disclosed";
  const signal = (row) => {
    if (row.reason === "raise_above_target_stage") return "Raise is above our target stage";
    if (row.reason === "pooled_or_investment_fund") return "Fund or investment vehicle";
    if (row.reason === "real_estate_or_investment_vehicle") return "Real estate or investment vehicle";
    if (row.reason === "missing_issuer_identity") return "Incomplete issuer information";
    if (String(row.reason).includes("venture_relevant_industry")) return "Target-sector operating company";
    return "Needs a quick partner review";
  };
  return (
    <>
      <header>
        <p className="eyebrow">SEC fundraising signals</p>
        <h1>Form D Deal Flow</h1>
        <p>Fresh startup fundraising signals from SEC filings, filtered to Glasswing's target stage.</p>
      </header>
      <section className="crawler-command-bar">
        <div><span className={`dot ${data?.desired ? "" : "paused"}`} /><div><strong>{data?.desired ? "EDGAR monitoring enabled" : "EDGAR monitoring paused"}</strong><span>Checks the SEC every 10 minutes · fair-access limited</span></div></div>
        <div className="crawler-command-actions">
          <button className="primary" disabled={busy || data?.desired} onClick={() => control(true)}><Play size={15}/> Start</button>
          <button disabled={busy || !data?.desired} onClick={() => control(false)}><Pause size={15}/> Pause</button>
          <button className="icon" disabled={busy} title="Pull latest SEC filings now" onClick={refresh}><RefreshCw size={15}/></button>
        </div>
      </section>
      {result.loading && !data ? <Loading /> : result.error && !data ? <ErrorBox>{result.error}</ErrorBox> : data && <>
        <div className="metrics form-d-metrics">
          <Metric label="New leads" value={fmt(data.counts?.investable || 0)} />
          <Metric label="Quick review" value={fmt(data.counts?.review || 0)} />
          <Metric label="Domains found" value={fmt(data.counts?.domains || 0)} />
          <Metric label="Automatically filtered" value={fmt(data.counts?.denied || 0)} />
        </div>
        <BucketTabs value={bucket} onChange={setBucket} tabs={[["investable", "New leads"], ["review", "Quick review"], ["denied", "Filtered out"]]} />
        <Table rows={data.rows || []} columns={[
          { key: "company_name", label: "Issuer", sortable: false },
          { key: "domain", label: "Website", sortable: false, render: (row) => row.domain ? <a href={`https://${row.domain}`} target="_blank" rel="noreferrer">{row.domain}</a> : <span className="muted">{row.domain_status === "unresolved" ? "Not verified" : "Resolving…"}</span> },
          { key: "filing_date", label: "Filed" },
          { key: "industry", label: "Industry" },
          { key: "offering_amount", label: "Target raise", render: (row) => money(row.offering_amount) },
          { key: "amount_sold", label: "Raised", render: (row) => money(row.amount_sold) },
          { key: "investors", label: "Investors" },
          { key: "reason", label: "Signal", sortable: false, render: signal },
          { key: "filing_url", label: "Filing", sortable: false, render: (row) => <a href={row.filing_url} target="_blank" rel="noreferrer">Open SEC filing</a> },
        ]} />
      </>}
    </>
  );
}

function SearchCorpusPage({ kind }) {
  const { admin, requestLogin } = useAdmin();
  const isAi = kind === "ai";
  const [metricsTick, setMetricsTick] = useState(0);
  const [controlBusy, setControlBusy] = useState(false);
  const metrics = useApi(`/api/corpus/${kind}/stats`, [kind, metricsTick]);
  const runtime = useApi("/api/runtime", [metricsTick]);
  const [upload, setUpload] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [quickDomain, setQuickDomain] = useState("");
  const [quickResult, setQuickResult] = useState(null);
  const [quickBusy, setQuickBusy] = useState(false);
  const crawlerUpdatedAt = Date.parse(metrics.data?.updated_at || "");
  const crawlerStale = Boolean(
    isAi &&
    Number(metrics.data?.pending || 0) > 0 &&
    Number.isFinite(crawlerUpdatedAt) &&
    Date.now() - crawlerUpdatedAt > 120000,
  );
  useEffect(() => {
    if (!metrics.data?.pending) return;
    const timer = setInterval(
      () => {
        if (document.visibilityState === "visible") {
          setMetricsTick((value) => value + 1);
        }
      },
      15000,
    );
    return () => clearInterval(timer);
  }, [metrics.data?.pending]);
  const serviceKey = isAi ? "ai_crawler" : "zone_crawler";
  const service = runtime.data?.services?.[serviceKey] || {};
  const desiredRunning = Boolean(service.desired);
  const controlCrawler = async (desired) => {
    if (!admin) return requestLogin();
    setControlBusy(true);
    try {
      await api(`/api/runtime/${serviceKey}`, {
        method: "POST",
        body: JSON.stringify({ desired }),
      });
      setMetricsTick((value) => value + 1);
    } finally {
      setControlBusy(false);
    }
  };
  return (
    <>
      <header>
        <p className="eyebrow">Crawler corpus</p>
        <h1>{isAi ? "Domain Intake" : "Domain Crawler (.com, .app)"}</h1>
        <p>
          {isAi
            ? "Paste one website or upload a domain list. Every domain follows the same scraping, scoring, bucketing, and Claude-review workflow."
            : "Rank newly observed domains, discard low-value sites, and surface only investable deal flow."}
        </p>
      </header>
      <section className="crawler-command-bar">
        <div>
          <span className={`dot ${desiredRunning ? "" : "paused"}`} />
          <div>
            <strong>{desiredRunning ? "Crawler enabled" : "Crawler paused"}</strong>
            <span>
              {String(
                metrics.data?.crawler_phase ||
                metrics.data?.crawler_status ||
                service.actual_status ||
                (desiredRunning ? "starting" : "paused")
              ).replaceAll("_", " ")}
            </span>
          </div>
        </div>
        <div className="crawler-command-actions">
          <button
            className="primary"
            disabled={controlBusy || desiredRunning || runtime.loading}
            onClick={() => controlCrawler(true)}
          >
            <Play size={15} /> Start
          </button>
          <button
            disabled={controlBusy || !desiredRunning || runtime.loading}
            onClick={() => controlCrawler(false)}
          >
            <Pause size={15} /> Pause
          </button>
          <button className="icon" title="Refresh crawler state" onClick={() => setMetricsTick((value) => value + 1)}>
            <RefreshCw size={15} />
          </button>
        </div>
      </section>
      {!isAi && <CrawlerDiscoveries />}
      {metrics.loading && !metrics.data ? (
        <Loading />
      ) : metrics.error && !metrics.data ? (
        <ErrorBox>Metrics unavailable: {metrics.error}</ErrorBox>
      ) : (
        <div className={`metrics corpus-metrics ${isAi ? "" : "retention-metrics"}`}>
          {isAi ? (
            <>
              <Metric label="Domains available" value={fmt(metrics.data.available)} />
              <Metric label="Waiting to scrape" value={fmt(metrics.data.pending)} />
              <Metric label="Successfully scraped" value={fmt(metrics.data.crawled)} />
              <Metric label="Failed" value={fmt(metrics.data.failed)} />
            </>
          ) : (
            <>
              <Metric label="New domains today" value={fmt(metrics.data.new_today)} />
              <Metric label="Skipped — low priority" value={fmt(metrics.data.quarantined)} />
              <Metric label="Domains checked" value={fmt(metrics.data.attempted)} />
              <Metric label="Passed quick screen" value={fmt(metrics.data.promising)} />
              <Metric label="Successfully scraped" value={fmt(metrics.data.scraped)} />
              <Metric label="Failed to reach" value={fmt(metrics.data.failed)} />
            </>
          )}
        </div>
      )}
      {!isAi && Boolean(metrics.data?.daily_intake) && (
        <div className="metrics daily-intake">
          {["yesterday", "today"].map((label) => {
            const view = metrics.data.daily_intake?.[label];
            const byTld = view?.by_tld || {};
            const tlds = Object.keys(byTld).sort();
            return (
              <div className="intake-view" key={label}>
                <h3>{label === "today" ? "Pulled in today" : "Pulled in yesterday"}</h3>
                <div className="intake-total">
                  {view?.imported ? fmt(view.new_domains) : "—"}
                </div>
                <div className="intake-detail">
                  {view?.imported
                    ? tlds
                        .map((tld) => `.${tld} ${fmt(byTld[tld].new_domains)}`)
                        .join(" · ")
                    : "no snapshot imported"}
                  {view?.day ? ` · ${view.day}` : ""}
                </div>
              </div>
            );
          })}
        </div>
      )}
      {!isAi && Boolean(metrics.data?.zone_deltas?.length) && (
        <>
          <div className="zone-delta-summary">
            {metrics.data.zone_deltas.map((zone) => (
              <span key={zone.tld}>
                <strong>.{zone.tld}</strong>
                {zone.is_baseline
                  ? ` baseline of ${fmt(zone.snapshot_domains)} domains — nothing to diff against yet, first daily comparison runs tomorrow`
                  : zone.is_today
                    ? ` +${fmt(zone.new_domains)} new today (vs ${fmt(zone.unchanged_domains)} already known)`
                    : ` last snapshot +${fmt(zone.new_domains)}`}
                {zone.snapshot_at
                  ? ` · ${new Date(zone.snapshot_at).toLocaleString()}`
                  : ""}
              </span>
            ))}
          </div>
          {metrics.data.snapshot_error && (
            <ErrorBox>
              Daily snapshot failed: {metrics.data.snapshot_error}
              {metrics.data.last_snapshot_attempt_at
                ? ` · attempted ${new Date(metrics.data.last_snapshot_attempt_at).toLocaleString()}`
                : ""}
            </ErrorBox>
          )}
        </>
      )}
      {isAi && metrics.data && (
        <section className={`corpus-status ${crawlerStale ? "stale" : ""}`}>
          <strong>
            Intake crawler: {crawlerStale
              ? "needs restart"
              : String(metrics.data.crawler_status || "unknown").replaceAll("_", " ")}
          </strong>
          <span>
            {fmt(metrics.data.processed)} processed total
            {metrics.data.updated_at
              ? ` · last worker update ${new Date(metrics.data.updated_at).toLocaleString()}`
              : ""}
          </span>
          {crawlerStale && <span>No heartbeat received for two minutes.</span>}
          {metrics.data.last_error && (
            <span className="error-text">Last error: {metrics.data.last_error}</span>
          )}
        </section>
      )}
      {isAi && (
            <section className="intake-panel">
              <div className="quick-intake">
                <Globe2 size={24} />
                <div>
                  <h3>Check a domain</h3>
                  <p>Paste a domain or full website URL.</p>
                  <form onSubmit={async (event) => {
                    event.preventDefault();
                    if (!admin) return requestLogin();
                    const validated = validateDomainInput(quickDomain);
                    if (validated.error) {
                      setQuickResult({ error: validated.error });
                      return;
                    }
                    setQuickBusy(true);
                    setQuickResult(null);
                    try {
                      const response = await api("/api/ai/domain", {
                        method: "POST",
                        body: JSON.stringify({ domain: validated.domain }),
                      });
                      setQuickResult(response);
                      setQuickDomain("");
                      setMetricsTick((value) => value + 1);
                    } catch (error) {
                      setQuickResult({ error: error.payload?.detail || error.message });
                    } finally {
                      setQuickBusy(false);
                    }
                  }}>
                    <input
                      type="text"
                      inputMode="url"
                      value={quickDomain}
                      onChange={(event) => {
                        setQuickDomain(event.target.value);
                        if (quickResult?.error) setQuickResult(null);
                      }}
                      placeholder="company.com"
                      aria-label="Domain or website URL"
                      required
                    />
                    <button className="primary" type="submit" disabled={quickBusy || !quickDomain.trim()}>
                      {quickBusy ? <RefreshCw className="spin" size={16} /> : <Play size={16} />}
                      {quickBusy ? "Processing…" : "Run through platform"}
                    </button>
                  </form>
                  {!admin && <button onClick={requestLogin}>Admin login to submit</button>}
                  {quickResult?.error && <p className="upload-error">{quickResult.error}</p>}
                  {quickResult && !quickResult.error && (
                    <div className="upload-result realtime-result">
                      <div>
                        <strong>{quickResult.score}/100</strong>
                        <span className="pill">{quickResult.bucket}</span>
                      </div>
                      <strong>{quickResult.company_name || quickResult.domain}</strong>
                      <span>{quickResult.reason}</span>
                      <span>{quickResult.description}</span>
                    </div>
                  )}
                </div>
              </div>
              <div className="upload bulk-intake">
                <Upload />
                <div>
                <h3>Upload domains</h3>
                <input
                  type="file"
                  accept=".csv,.txt"
                  disabled={!admin}
                  onChange={async (e) => {
                    const f = e.target.files[0];
                    if (!f) return;
                    setUploading(true);
                    setUpload(null);
                    const body = new FormData();
                    body.append("file", f);
                    try {
                      const r = await api("/api/ai/upload", {
                        method: "POST",
                        body,
                      });
                      setUpload(r);
                      setMetricsTick((value) => value + 1);
                    } catch (error) {
                      setUpload({ error: error.message });
                    } finally {
                      setUploading(false);
                      e.target.value = "";
                    }
                  }}
                />
                {!admin && (
                  <button onClick={requestLogin}>Admin login to upload</button>
                )}
                {uploading && <p>Reading file and adding new domains…</p>}
                {upload?.error && (
                  <p className="upload-error">{upload.error}</p>
                )}
                {upload && !upload.error && (
                  <div className="upload-result">
                    <strong>{fmt(upload.added)} new domains queued</strong>
                    <span>
                      {fmt(upload.received)} unique received ·{" "}
                      {fmt(upload.duplicates)} already known
                    </span>
                    <span>
                      They move from Waiting to scrape → Crawled → Searchable
                      automatically.
                    </span>
                  </div>
                )}
              </div>
              </div>
            </section>
      )}
      <CorpusCompanies kind={kind} />
    </>
  );
}

function HealthPage() {
  const { admin, requestLogin } = useAdmin();
  const [full, setFull] = useState(false);
  const [tick, setTick] = useState(0);
  const health = useApi(`/api/health?full=${full}&refresh=${tick}`, [full, tick]);
  if (health.loading && !health.data) return <Loading />;
  if (health.error && !health.data) return <ErrorBox>{health.error}</ErrorBox>;
  const d = health.data;
  const services = d.runtime?.services || {};
  const control = async (service, desired) => {
    if (!admin) return requestLogin();
    await api(`/api/runtime/${service}`, {
      method: "POST",
      body: JSON.stringify({ desired }),
    });
    setTick((x) => x + 1);
  };
  return (
    <>
      <header>
        <p className="eyebrow">Operations</p>
        <h1>System Health</h1>
        <p>
          Worker controls and database health without rerunning unrelated views.
        </p>
      </header>
      <div className="section-title">
        <span className={`status ${String(d.overall).toLowerCase()}`}>
          {d.overall}
        </span>
        <button
          disabled={health.loading}
          onClick={() => {
            setFull(true);
            setTick((value) => value + 1);
          }}
        >
          {health.loading ? "Running full checks…" : "Run full checks"}
        </button>
      </div>
      <div className="metrics">
        <Metric label="Companies" value={fmt(d.counts.classifications)} />
        <Metric
          label="Human decisions"
          value={fmt(d.counts.manual_decisions)}
        />
        <Metric
          label="Pipeline pending"
          value={fmt(d.counts.pipeline_pending)}
        />
        <Metric label=".AI searchable" value={fmt(d.counts.ai_indexed)} />
        <Metric
          label=".COM/.APP searchable"
          value={fmt(d.counts.zone_indexed)}
        />
      </div>
      <section>
        <div className="section-title">
          <h2>Workers</h2>
          <div>
            <button onClick={() => control("all", true)}>
              <Play size={15} />
              Resume all
            </button>
            <button
              onClick={() =>
                window.confirm("Pause every worker safely?") &&
                control("all", false)
              }
            >
              <Pause size={15} />
              Pause all
            </button>
          </div>
        </div>
        <div className="worker-list">
          {Object.entries(services).map(([key, row]) => (
            <div className="worker" key={key}>
              <div>
                <strong>{key.replaceAll("_", " ")}</strong>
                <span>
                  {row.desired ? "Enabled" : "Paused"} · actual: {String(row.actual_status || "unknown").replaceAll("_", " ")}
                </span>
              </div>
              <div>
                <button
                  disabled={row.desired}
                  onClick={() => control(key, true)}
                >
                  Start
                </button>
                <button
                  disabled={!row.desired}
                  onClick={() => control(key, false)}
                >
                  Pause
                </button>
              </div>
            </div>
          ))}
        </div>
      </section>
      <section>
        <h2>Health checks</h2>
        <Table
          rows={d.checks || []}
          columns={[
            { key: "Check", label: "Check" },
            {
              key: "Status",
              label: "Status",
              render: (row) => (
                <span className={`status ${String(row.Status).toLowerCase()}`}>
                  {row.Status}
                </span>
              ),
            },
            { key: "Detail", label: "Detail" },
          ]}
        />
      </section>
      {admin && <AdminActivity />}
    </>
  );
}

function AdminActivity() {
  const activity = useApi("/api/admin/audit?limit=100", []);
  return (
    <section>
      <div className="table-toolbar">
        <div>
          <h2>Admin activity</h2>
          <p>Append-only history of operational and review actions.</p>
        </div>
      </div>
      {activity.loading && !activity.data ? (
        <Loading />
      ) : activity.error && !activity.data ? (
        <ErrorBox>{activity.error}</ErrorBox>
      ) : (
        <Table
          rows={activity.data.rows}
          columns={[
            { key: "created_at", label: "Time" },
            { key: "action", label: "Action" },
            { key: "target", label: "Target" },
            { key: "detail", label: "Detail" },
          ]}
        />
      )}
    </section>
  );
}

function ClaudeQueuePage() {
  const { admin, requestLogin } = useAdmin();
  const [tick, setTick] = useState(0);
  const [overrideDomain, setOverrideDomain] = useState("");
  const [message, setMessage] = useState("");
  const queue = useApi("/api/claude/queue", [tick]);
  useEffect(() => {
    const timer = setInterval(() => {
      if (document.visibilityState === "visible") setTick((v) => v + 1);
    }, 5000);
    return () => clearInterval(timer);
  }, []);
  const retry = async (domain) => {
    if (!admin) return requestLogin();
    try {
      await requestClaudeReview(domain);
      setMessage(`${domain} queued for retry`);
      setTick((v) => v + 1);
    } catch (error) {
      if (error.status === 429) setOverrideDomain(domain);
      else setMessage(`Retry failed: ${error.message}`);
    }
  };
  const d = queue.data || { rows: [], counts: {} };
  return (
    <>
      <header>
        <p className="eyebrow">External diligence</p>
        <h1>Claude Queue</h1>
        <p>
          Durable paid-review jobs continue through navigation and deployments.
        </p>
      </header>
      <div className="metrics">
        <Metric label="Queued" value={fmt(d.counts.queued)} />
        <Metric label="Reviewing" value={fmt(d.counts.reviewing)} />
        <Metric label="Completed" value={fmt(d.counts.completed)} />
        <Metric label="Failed" value={fmt(d.counts.failed)} />
      </div>
      {message && <p className="notice">{message}</p>}
      <div className="inline-tools">
        <button
          onClick={async () => {
            if ("Notification" in window)
              await Notification.requestPermission();
          }}
        >
          Enable completion notifications
        </button>
        <button onClick={() => setTick((v) => v + 1)}>
          <RefreshCw size={15} />
          Refresh
        </button>
      </div>
      {queue.loading && !queue.data ? (
        <Loading />
      ) : queue.error && !queue.data ? (
        <ErrorBox>{queue.error}</ErrorBox>
      ) : (
        <Table
          rows={d.rows}
          columns={[
            {
              key: "domain",
              label: "Domain",
              render: (row) => (
                <a href={`https://${row.domain}`} target="_blank">
                  {row.domain}
                </a>
              ),
            },
            {
              key: "status",
              label: "Status",
              render: (row) => (
                <span className={`status ${row.status}`}>{row.status}</span>
              ),
            },
            { key: "decision", label: "Decision" },
          { key: "fit_score", label: "Claude score" },
          {
            key: "rubric",
            label: "Rubric",
            render: (row) => row.thesis_fit !== undefined ? (
              <span className="rubric-summary" title={claudeScorecard({
                ...row,
                claude_reasoning: row.reasoning,
                claude_concerns: row.concerns,
                claude_unknowns: row.unknowns,
                claude_dimensions: row,
              })}>
                T{row.thesis_fit} · E{row.enterprise_value} · P{row.product_depth} · M{row.technical_moat} · S{row.stage_fit}
              </span>
            ) : "Legacy review",
          },
            { key: "requested_at", label: "Requested" },
            { key: "error", label: "Error" },
            {
              key: "retry",
              label: "Action",
              render: (row) =>
                row.status === "failed" ? (
                  <button onClick={() => {
                    if (String(row.error || "").toLowerCase().includes("budget"))
                      setOverrideDomain(row.domain);
                    else retry(row.domain);
                  }}>
                    {String(row.error || "").toLowerCase().includes("budget")
                      ? "Override & retry" : "Retry"}
                  </button>
                ) : (
                  ""
                ),
            },
          ]}
        />
      )}
      {overrideDomain && (
        <ClaudeOverrideModal
          domain={overrideDomain}
          onClose={() => setOverrideDomain("")}
          onOverride={async () => {
            await requestClaudeReview(overrideDomain, true);
            setMessage(`${overrideDomain} queued with daily-limit override`);
            setOverrideDomain("");
            setTick((value) => value + 1);
          }}
        />
      )}
    </>
  );
}

function ClaudeNotifier() {
  const { admin } = useAdmin();
  const seen = useRef({});
  useEffect(() => {
    if (!admin) return;
    const check = async () => {
      try {
        const data = await api("/api/claude/queue?limit=50");
        for (const row of data.rows) {
          const before = seen.current[row.domain];
          if (
            before &&
            before !== row.status &&
            ["completed", "failed"].includes(row.status) &&
            "Notification" in window &&
            Notification.permission === "granted"
          )
            new Notification(`Claude review ${row.status}`, {
              body: `${row.domain}${row.decision ? ` · ${row.decision}` : ""}`,
            });
          seen.current[row.domain] = row.status;
        }
      } catch {}
    };
    check();
    const timer = setInterval(() => {
      if (document.visibilityState === "visible") check();
    }, 10000);
    return () => clearInterval(timer);
  }, [admin]);
  return null;
}

function GlobalWorkerControls() {
  const { admin } = useAdmin();
  const [tick, setTick] = useState(0);
  const [busy, setBusy] = useState(false);
  const runtime = useApi("/api/runtime", [tick]);
  useEffect(() => {
    if (!admin) return;
    const timer = setInterval(() => {
      if (document.visibilityState === "visible") {
        setTick((value) => value + 1);
      }
    }, 30000);
    return () => clearInterval(timer);
  }, [admin]);
  if (!admin) return null;
  const services = runtime.data?.services || {};
  const values = Object.values(services);
  const allRunning = values.length > 0 && values.every((row) => row.desired);
  const allPaused = values.length > 0 && values.every((row) => !row.desired);
  const activeCount = values.filter((row) =>
    row.desired && /running|screening|crawling|importing|starting|listening|idle|waiting/.test(
      String(row.actual_status || "").toLowerCase(),
    )
  ).length;
  const control = async (desired) => {
    if (!desired && !window.confirm("Pause every worker safely?")) return;
    setBusy(true);
    try {
      await api("/api/runtime/all", {
        method: "POST",
        body: JSON.stringify({ desired }),
      });
      setTick((value) => value + 1);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="global-controls" aria-label="Global worker controls">
      <div>
        <span className={`dot ${allPaused ? "paused" : ""}`} />
        <strong>
          {allRunning
            ? `All workers enabled · ${activeCount}/${values.length} active`
            : allPaused
              ? "All workers paused"
              : "Mixed worker state"}
        </strong>
      </div>
      <div>
        <button
          disabled={busy || allRunning || runtime.loading}
          onClick={() => control(true)}
        >
          <Play size={15} /> Resume all
        </button>
        <button
          disabled={busy || allPaused || runtime.loading}
          onClick={() => control(false)}
        >
          <Pause size={15} /> Pause all
        </button>
        <button
          className="icon"
          disabled={busy}
          onClick={() => setTick((value) => value + 1)}
          title="Refresh worker state"
        >
          <RefreshCw size={15} />
        </button>
      </div>
    </div>
  );
}

function CompetitiveLandscapePage() {
  const { admin, requestLogin } = useAdmin();
  const [website, setWebsite] = useState(() => {
    const value = sessionStorage.getItem("landscape_prefill") || "";
    sessionStorage.removeItem("landscape_prefill");
    return value;
  });
  const [tick, setTick] = useState(0);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const jobs = useApi(`/api/competitive-landscapes?t=${tick}`, [tick]);
  const active = (jobs.data?.jobs || []).some((job) => ["queued", "researching", "exporting"].includes(job.status));
  useEffect(() => {
    if (!active) return undefined;
    const timer = setInterval(() => {
      if (document.visibilityState === "visible") {
        setTick((value) => value + 1);
      }
    }, 5000);
    return () => clearInterval(timer);
  }, [active]);
  const submit = async (event) => {
    event.preventDefault();
    if (!admin) return requestLogin();
    setBusy(true); setError(""); setMessage("");
    const formElement = event.currentTarget;
    const form = new FormData(formElement);
    const attachments = form.getAll("materials").filter((file) => file?.name && file.size > 0);
    form.delete("materials");
    attachments.forEach((file) => form.append("materials", file));
    try {
      const result = await api("/api/competitive-landscapes", { method: "POST", body: form });
      setMessage(`Research job ${result.job_id.slice(0, 8)} is queued. You can leave this page; it will keep running.`);
      formElement.reset();
      setWebsite("");
      setTick((value) => value + 1);
    } catch (err) {
      setError(err.payload?.detail || err.message || "Upload failed");
    } finally { setBusy(false); }
  };
  const rows = jobs.data?.jobs || [];
  const completed = rows.filter((job) => job.status === "completed").length;
  const retry = async (jobId) => {
    if (!admin) return requestLogin();
    setError("");
    try {
      await api(`/api/competitive-landscapes/${jobId}/retry`, { method: "POST" });
      setTick((value) => value + 1);
    } catch (err) {
      setError(err.payload?.detail || err.message || "Retry failed");
    }
  };
  return <section className="landscape-page">
    <header>
      <p className="eyebrow">MARKET INTELLIGENCE</p>
      <h1>Competitive Landscape</h1>
      <p>Turn a company website and supporting materials into a sourced direct-competitor workbook using the Talvy template.</p>
    </header>
    <div className="metrics">
      <div className="metric"><span>Total analyses</span><strong>{fmt(rows.length)}</strong></div>
      <div className="metric"><span>In progress</span><strong>{fmt(rows.length - completed - rows.filter((j) => j.status === "failed").length)}</strong></div>
      <div className="metric"><span>Completed</span><strong>{fmt(completed)}</strong></div>
      <div className="metric"><span>Competitors mapped</span><strong>{fmt(rows.reduce((sum, job) => sum + Number(job.competitor_count || 0), 0))}</strong></div>
    </div>
    <form className="landscape-form" onSubmit={submit}>
      <div className="section-title"><div><h2>Build a landscape</h2><p>Claude researches the market and fills every applicable field without fabricating unknown data.</p></div></div>
      <div className="form-grid">
        <label className="wide"><span>Company website *</span><input name="website" type="text" inputMode="url" placeholder="https://company.com" value={website} onChange={(event) => setWebsite(event.target.value)} required /></label>
        <label className="wide"><span>Competitors</span><input name="requested_count" type="number" min="5" max="75" defaultValue="25" required /></label>
        <label className="wide"><span>Supporting materials <small>optional · PDF, DOCX, XLSX, CSV, TXT</small></span><input name="materials" type="file" accept=".pdf,.docx,.xlsx,.csv,.txt,.md" multiple /></label>
        <label className="wide"><span>Research notes <small>optional</small></span><textarea name="notes" rows="3" placeholder="Product positioning, geographies, competitor types, or special questions…" /></label>
      </div>
      <div className="form-actions">
        <button className="primary" type="submit" disabled={busy}>{busy ? <><RefreshCw className="spin" size={16}/> Uploading…</> : <><FileSpreadsheet size={16}/> Build landscape</>}</button>
        {!admin && <span>Admin login is required to start paid research.</span>}
      </div>
      {message && <p className="notice inline-notice">{message}</p>}
      {error && <p className="error inline-notice">{error}</p>}
    </form>
    <div className="table-heading"><div><h2>Analysis history</h2><p>Research continues in the background through navigation and browser closes.</p></div><button onClick={() => setTick((value) => value + 1)}><RefreshCw size={15}/> Refresh</button></div>
    {jobs.loading && !jobs.data ? <p className="loading"><RefreshCw size={15}/> Loading analyses…</p> :
      rows.length === 0 ? <p className="empty">No competitive landscapes yet.</p> :
      <div className="table-wrap"><table><thead><tr><th>Company</th><th>Website</th><th>Status</th><th>Competitors</th><th>Requested</th><th>Completed</th><th>Action</th></tr></thead><tbody>
      {rows.map((job) => <tr key={job.job_id}><td><strong>{job.company_name || "Researching…"}</strong></td><td>{job.website}</td><td><span className={`pill job-${job.status}`}>{job.status}</span>{job.error && <small className="row-error">{job.error}</small>}</td><td>{fmt(job.competitor_count || 0)}</td><td>{new Date(job.created_at).toLocaleString()}</td><td>{job.completed_at ? new Date(job.completed_at).toLocaleString() : "—"}</td><td>{job.status === "completed" ? <a className="button-link" href={`/api/competitive-landscapes/${job.job_id}/download`}><Download size={15}/> Download</a> : job.status === "failed" ? <button onClick={() => retry(job.job_id)}><RefreshCw size={15}/> Retry</button> : "—"}</td></tr>)}
      </tbody></table></div>}
  </section>;
}

const FOUNDER_VIEWS = [
  ["priority", "Priority Signals"],
  ["universe", "Talent Universe"],
  ["activity", "Activity Monitor"],
  ["identity", "Identity Review"],
  ["settings", "Settings"],
];
function FounderProfile({ candidateId, onClose, onChanged }) {
  const { admin, requestLogin } = useAdmin();
  const profile = useApi(`/api/founder-signals/candidates/${candidateId}`, [candidateId]);
  const [busy, setBusy] = useState(false);
  if (profile.loading && !profile.data) return <div className="modal-backdrop"><div className="modal"><Loading /></div></div>;
  const row = profile.data;
  if (!row) return null;
  const act = async (action, value = "") => {
    if (!admin) return requestLogin();
    setBusy(true);
    try {
      await api(`/api/founder-signals/candidates/${candidateId}/action`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action, value }) });
      onChanged(); onClose();
    } finally { setBusy(false); }
  };
  return <div className="modal-backdrop" onMouseDown={onClose}><div className="modal founder-profile" onMouseDown={(event) => event.stopPropagation()}>
    <button className="modal-close" onClick={onClose}><XCircle size={20}/></button>
    <p className="eyebrow">FOUNDER PROFILE</p><h2>{row.full_name}</h2>
    <p>{row.current_title}{row.current_employer ? ` · ${row.current_employer}` : ""}</p>
    <div className="founder-score-grid">
      {[['Priority',row.overall_priority_score],['Transition',row.founder_transition_score],['Talent',row.talent_score],['Glasswing fit',row.glasswing_fit_score],['Identity',row.identity_confidence]].map(([label,value]) => <div key={label}><span>{label}</span><strong>{value}/100</strong></div>)}
    </div>
    <div className="founder-profile-grid">
      <div><h3>Professional profile</h3><dl><dt>School / lab</dt><dd>{[row.school,row.research_laboratory].filter(Boolean).join(" · ") || "Not verified"}</dd><dt>Location</dt><dd>{row.geographic_location || "Not verified"}</dd><dt>Technical focus</dt><dd>{(row.technical_categories || []).join(", ") || "Not set"}</dd><dt>GitHub</dt><dd>{row.github_username ? <a href={row.github_profile_url} target="_blank" rel="noreferrer">@{row.github_username}</a> : "Unresolved"}</dd></dl></div>
      <div><h3>Activity</h3><div className="activity-bars">{(row.activity_monthly || []).slice(0,6).reverse().map((month) => <div key={month.metric_month} title={`${month.total_activity} events`}><span style={{height:`${Math.max(5,Math.min(100,month.total_activity*4))}%`}}/><small>{month.metric_month.slice(5)}</small></div>)}</div></div>
    </div>
    <h3>Verified pedigree</h3>{row.pedigree?.length ? <div className="signal-list">{row.pedigree.map((item) => <article key={item.evidence_id}><strong>{item.label}</strong><span className={`pill ${item.verification_status === "verified" ? "keep" : "review"}`}>{item.verification_status === "verified" ? "Verified" : "Needs review"} · {item.confidence}%</span><p>{item.evidence}</p><a href={item.source_url} target="_blank" rel="noreferrer">{String(item.provider).replaceAll("_"," ")}</a></article>)}</div> : <p className="empty">No non-GitHub pedigree evidence collected yet.</p>}
    <h3>Signal history</h3>{row.signals?.length ? <div className="signal-list">{row.signals.map((signal) => <article key={signal.signal_id}><strong>{String(signal.signal_type).replaceAll("_"," ")}</strong><span>{new Date(signal.event_at).toLocaleDateString()}</span><p>{signal.explanation}</p><small>{signal.evidence}</small></article>)}</div> : <p className="empty">No credible founder-transition signal yet.</p>}
    <h3>Collaborators</h3>{row.relationships?.length ? <table><tbody>{row.relationships.map((item) => <tr key={item.relationship_id}><td>{item.collaborator_name || item.collaborator_username}</td><td>{item.relationship_type}</td><td>{item.strength}</td></tr>)}</tbody></table> : <p className="muted">No public collaborator links detected.</p>}
    <div className="form-actions">
      <select defaultValue={row.analyst_status} onChange={(event) => act("status",event.target.value)} disabled={busy}>{["new","monitoring","priority","contact_now","watch","needs_research","false_positive","not_relevant","archived"].map((value)=><option key={value}>{value}</option>)}</select>
      <button onClick={()=>act("monitoring",row.monitoring_status === "active" ? "inactive" : "active")} disabled={busy}>{row.monitoring_status === "active" ? "Stop monitoring" : "Start monitoring"}</button>
      <button onClick={()=>act("outreach","completed")} disabled={busy}>Mark outreach completed</button>
    </div>
  </div></div>;
}
function FounderSignalsPage() {
  const { admin, requestLogin } = useAdmin();
  const [view, setView] = useState("priority");
  const [tick, setTick] = useState(0);
  const [selected, setSelected] = useState("");
  const [query, setQuery] = useState("");
  const [minimumScore, setMinimumScore] = useState(0);
  const [sort, setSort] = useState("priority");
  const [message, setMessage] = useState("");
  const overview = useApi("/api/founder-signals/overview", [tick]);
  const candidateUrl = `/api/founder-signals/candidates?page_size=100&minimum_score=${view === "priority" ? Math.max(50,minimumScore) : minimumScore}&sort=${sort}&q=${encodeURIComponent(query)}`;
  const candidates = useApi(candidateUrl, [tick, view, query, minimumScore, sort]);
  const identities = useApi("/api/founder-signals/identity-review", [tick]);
  const settings = useApi("/api/founder-signals/settings", [tick]);
  const jobs = useApi("/api/founder-signals/jobs", [tick]);
  const rows = candidates.data?.rows || [];
  const mutate = async (path, options = {}) => { if (!admin) return requestLogin(); setMessage(""); try { const result = await api(path, options); setTick((value)=>value+1); return result; } catch (err) { setMessage(err.payload?.detail || err.message); } };
  const upload = async (event) => { const file=event.target.files?.[0]; if(!file)return; const body=new FormData(); body.append("file",file); const result=await mutate("/api/founder-signals/import",{method:"POST",body}); if(result)setMessage(`${result.created} people imported · ${result.identity_review} possible duplicates need review`); event.target.value=""; };
  const addPerson = async (event) => { event.preventDefault(); const form=new FormData(event.currentTarget); const values=Object.fromEntries(form.entries()); values.technical_categories=String(values.technical_categories||"").split(",").map(v=>v.trim()).filter(Boolean); const result=await mutate("/api/founder-signals/candidates",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({values})}); if(result?.status==="needs_review")setMessage("Possible duplicate found. Review the existing identity before creating another record."); else event.currentTarget.reset(); };
  const runJob = async (job_type) => { const result=await mutate("/api/founder-signals/jobs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({job_type})}); if(result)setMessage(`${job_type.replaceAll("_"," ")} queued`); };
  const saveSettings = async (event) => { event.preventDefault(); const form=new FormData(event.currentTarget); const list=(name)=>String(form.get(name)||"").split("\n").map(v=>v.trim()).filter(Boolean); await mutate("/api/founder-signals/settings",{method:"PUT",headers:{"Content-Type":"application/json"},body:JSON.stringify({values:{target_companies:list("target_companies"),target_universities:list("target_universities"),target_laboratories:list("target_laboratories"),technical_categories:list("technical_categories"),minimum_alert_threshold:Number(form.get("minimum_alert_threshold")),collection_frequency_hours:Number(form.get("collection_frequency_hours")),maximum_people_per_run:Number(form.get("maximum_people_per_run")),automatic_monitoring_confidence:Number(form.get("automatic_monitoring_confidence"))}})}); };
  const o=overview.data?.counts||{};
  return <section className="founder-signals-page"><header><p className="eyebrow">EARLY FOUNDER FORMATION</p><h1>Founder Signals</h1><p>Public, evidence-backed signals that exceptional technical people may be preparing to build a Glasswing-relevant company.</p></header>
    <div className="metrics founder-metrics"><Metric label="Talent universe" value={fmt(o.total||0)}/><Metric label="Actively monitored" value={fmt(o.monitored||0)}/><Metric label="Priority signals" value={fmt(o.priority||0)}/><Metric label="Signals · 30 days" value={fmt(o.signals_30d||0)}/><Metric label="Identity review" value={fmt(o.identity_review||0)}/></div>
    <div className="founder-tabs">{FOUNDER_VIEWS.map(([id,label])=><button key={id} className={view===id?"active":""} onClick={()=>setView(id)}>{label}</button>)}</div>
    {message&&<p className="notice">{message}</p>}
    {(view==="priority"||view==="universe")&&<><div className="founder-toolbar"><div className="compact-search"><Search size={17}/><input value={query} onChange={(e)=>setQuery(e.target.value)} placeholder="Search people, employers, or GitHub"/></div><label>Minimum score <input type="number" min="0" max="100" value={minimumScore} onChange={(e)=>setMinimumScore(Number(e.target.value))}/></label><select value={sort} onChange={(e)=>setSort(e.target.value)}><option value="priority">Highest priority</option><option value="signal">Most recent signal</option><option value="transition">Founder transition</option><option value="talent">Talent quality</option><option value="fit">Glasswing fit</option></select><a className="button-link" href="/api/founder-signals/export"><Download size={15}/> Export CSV</a></div>
      {view==="universe"&&<div className="founder-admin-grid"><form onSubmit={addPerson}><h3>Add a person</h3><input name="full_name" placeholder="Full name" required/><input name="current_employer" placeholder="Current employer"/><input name="current_title" placeholder="Current title"/><input name="school" placeholder="School or lab"/><input name="github_username" placeholder="GitHub username (optional)"/><input name="technical_categories" placeholder="Categories, comma separated"/><button className="primary">Add person</button></form><label className="founder-upload"><Upload size={24}/><strong>Import talent universe</strong><span>CSV or Excel · ambiguous matches go to Identity Review</span><input type="file" accept=".csv,.xlsx" onChange={upload}/></label></div>}
      {candidates.loading&&!candidates.data?<Loading/>:rows.length?<div className="table-wrap"><table className="founder-table"><thead><tr><th>Person</th><th>Affiliation</th><th>Focus</th><th>Transition</th><th>Talent</th><th>GW fit</th><th>Priority</th><th>Identity</th><th>Status</th></tr></thead><tbody>{rows.map(row=><tr key={row.candidate_id} onClick={()=>setSelected(row.candidate_id)}><td><strong>{row.full_name}</strong><small>{row.github_username?`@${row.github_username}`:"GitHub unresolved"}{row.synthetic?" · SYNTHETIC":""}</small></td><td>{row.current_employer}<small>{row.current_title}</small></td><td>{(row.technical_categories||[]).slice(0,2).join(" · ")||"—"}</td><td>{row.founder_transition_score}</td><td>{row.talent_score}</td><td>{row.glasswing_fit_score}</td><td><strong className="priority-number">{row.overall_priority_score}</strong></td><td><span className={`pill confidence-${row.confidence_level}`}>{row.identity_confidence}%</span></td><td>{String(row.analyst_status).replaceAll("_"," ")}</td></tr>)}</tbody></table></div>:<p className="empty">No people match this view. Import a talent universe or seed synthetic examples in Settings.</p>}</>}
    {view==="activity"&&<><div className="crawler-command-bar"><div><strong>{overview.data?.github?.configured?"GitHub API connected":"GitHub token required"}</strong><span>GitHub tracks activity. OpenAlex and Semantic Scholar independently validate research pedigree.</span></div><div><button onClick={()=>runJob("talent_discovery")}><Users size={15}/> Discover talent</button><button onClick={()=>runJob("pedigree_enrichment")}><Search size={15}/> Enrich pedigree</button><button onClick={()=>runJob("github_collection")}><RefreshCw size={15}/> Collect now</button><button onClick={()=>runJob("score_recalculation")}>Recalculate scores</button></div></div><div className="table-wrap"><table><thead><tr><th>Job</th><th>Status</th><th>Started</th><th>Succeeded</th><th>Failed</th></tr></thead><tbody>{(jobs.data?.rows||[]).map(job=><tr key={job.job_id}><td>{job.job_type}</td><td>{job.status}</td><td>{job.started_at||job.created_at}</td><td>{job.succeeded}</td><td>{job.failed}</td></tr>)}</tbody></table></div></>}
    {view==="identity"&&<>{(identities.data?.rows||[]).length?<div className="identity-grid">{identities.data.rows.map(item=><article key={item.identity_id}><h3>{item.full_name}</h3><p>{item.current_title} · {item.current_employer}</p><a href={item.profile_url} target="_blank" rel="noreferrer">@{item.username}</a><strong>{item.confidence_score}% confidence</strong><ul>{item.evidence.map(value=><li key={value}>{value}</li>)}</ul>{item.contradictions.map(value=><p className="error" key={value}>{value}</p>)}<div><button onClick={()=>mutate(`/api/founder-signals/candidates/${item.candidate_id}/identity`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({username:item.username,accepted:true})})}>Approve</button><button onClick={()=>mutate(`/api/founder-signals/candidates/${item.candidate_id}/identity`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({username:item.username,accepted:false})})}>Reject</button></div></article>)}</div>:<p className="empty">No ambiguous GitHub identities need review.</p>}</>}
    {view==="settings"&&settings.data&&<><form className="founder-settings" onSubmit={saveSettings}><label>Target companies<textarea name="target_companies" defaultValue={(settings.data.target_companies||[]).join("\n")}/></label><label>Target universities<textarea name="target_universities" defaultValue={(settings.data.target_universities||[]).join("\n")}/></label><label>Target laboratories<textarea name="target_laboratories" defaultValue={(settings.data.target_laboratories||[]).join("\n")}/></label><label>Technical categories<textarea name="technical_categories" defaultValue={(settings.data.technical_categories||[]).join("\n")}/></label><label>Minimum alert score<input name="minimum_alert_threshold" type="number" min="0" max="100" defaultValue={settings.data.minimum_alert_threshold}/></label><label>Collection frequency (hours)<input name="collection_frequency_hours" type="number" min="1" defaultValue={settings.data.collection_frequency_hours}/></label><label>People per run<input name="maximum_people_per_run" type="number" min="1" max="5000" defaultValue={settings.data.maximum_people_per_run}/></label><label>Automatic identity threshold<input name="automatic_monitoring_confidence" type="number" min="70" max="100" defaultValue={settings.data.automatic_monitoring_confidence}/></label><button className="primary">Save settings</button></form><div className="form-actions"><span>GitHub API: {settings.data.github_api_configured?"Configured":"Not configured"} · token is never displayed</span><button onClick={()=>mutate("/api/founder-signals/seed-demo",{method:"POST"})}>Load synthetic demo data</button></div></>}
    {selected&&<FounderProfile candidateId={selected} onClose={()=>setSelected("")} onChanged={()=>setTick(v=>v+1)}/>}</section>;
}


const pct = (value) => `${(Number(value || 0) * 100).toFixed(1)}%`;
const num = (value) => Number(value || 0).toLocaleString();

// Survived and dropped are the two states a reader must tell apart, so they
// carry the colour. Pending is a hatched neutral rather than a third hue: it
// is not a decision at all, and keeping it out of the hue encoding is what
// lets the palette pass colour-vision separation. The design system's own red
// and a blue at ΔE 16.6 (protan) were checked with the palette validator --
// the obvious red/amber pair sat at ΔE 2.9, which would have made "rejected"
// and "still queued" identical to a red-green colourblind reader.
// A stat line, not a chart. Two numbers and their ratio is a hero-number job;
// plotting it would add a scale and a legend to communicate one comparison.
function Throughput({ windows }) {
  if (!windows?.length) return null;
  return (
    <div className="throughput">
      {windows.map((w) => {
        const label = w.window_hours === 24 ? "Last 24 hours" : "Last 7 days";
        const state = w.stalled ? "stalled" : w.keeping_up ? "ok" : "behind";
        const verdict = w.stalled
          ? "screening stopped"
          : w.keeping_up
            ? "screening keeps up"
            : `${w.overload_ratio}\u00d7 more arriving than screened`;
        return (
          <div key={w.window_hours} className={`throughput-card ${state}`}>
            <h3>{label}</h3>
            <p className="throughput-verdict">{verdict}</p>
            <dl>
              <div><dt>Arriving</dt><dd>{num(w.arrived)} <span>({w.arrival_per_min}/min)</span></dd></div>
              <div><dt>Fetched</dt><dd>{num(w.fetched)} <span>({w.fetch_per_min}/min)</span></dd></div>
            </dl>
          </div>
        );
      })}
    </div>
  );
}

// Each bar is full width and shows one stage's own split, rather than being
// scaled to the stage's share of the starting population. Measured against
// real data, proportional widths put the last four stages at ~1.2px -- the
// funnel discards 99.2% overall, so every stage past the third is invisible,
// and those are the ones worth reading. Log-scaling the width would restore
// the sizes while misrepresenting magnitude, so cumulative attrition is
// carried as an explicit "% of start" number instead.
function StageBar({ stage }) {
  const part = (n) => (stage.entered > 0 ? (n / stage.entered) * 100 : 0);
  return (
    <div className="funnel-track" role="img"
         aria-label={`${stage.label}: ${num(stage.dropped)} removed, ${num(stage.survived)} continued`}>
      <div className="funnel-bar">
        {stage.survived > 0 && (
          <span className="seg seg-survived" style={{ flexGrow: part(stage.survived) }}
                title={`Continued: ${num(stage.survived)}`} />
        )}
        {stage.dropped > 0 && (
          <span className="seg seg-dropped" style={{ flexGrow: part(stage.dropped) }}
                title={`Removed here: ${num(stage.dropped)} (${pct(stage.drop_rate)} of what reached this step)`} />
        )}
      </div>
    </div>
  );
}

function FunnelTable({ funnel }) {
  return (
    <table className="funnel-table">
      <thead>
        <tr>
          <th>Step</th>
          <th className="n">In</th>
          <th className="n">Out</th>
          <th className="n">Continued</th>
          <th className="n">Drop rate</th>
          {/* What this step shaved off the original population. The number
              that says which steps matter: a step removing 90% of a
              population already down to 0.1% of the top line is doing almost
              nothing to the funnel. */}
          <th className="n">% of top-line</th>
          <th>Split</th>
        </tr>
      </thead>
      <tbody>
        {funnel.stages.map((stage) => (
          <tr key={stage.key}>
            <td>
              <strong>{stage.label}</strong>
              {stage.note && <div className="funnel-note">{stage.note}</div>}
              {stage.reasons?.length > 0 && (
                <details className="funnel-reasons">
                  <summary>{stage.reasons.length} reasons</summary>
                  <ul>
                    {stage.reasons.map((reason) => (
                      <li key={reason.key}>
                        <span>{reason.label}</span>
                        <span className="n">
                          {num(reason.dropped)} · {pct(reason.share_of_stage)}
                        </span>
                      </li>
                    ))}
                  </ul>
                </details>
              )}
            </td>
            <td className="n">{num(stage.entered)}</td>
            <td className="n">{num(stage.dropped)}</td>
            <td className="n">{num(stage.survived)}</td>
            <td className="n">{stage.entered > 0 ? pct(stage.drop_rate) : "—"}</td>
            <td className="n">{pct(stage.shave_of_top)}</td>
            <td><StageBar stage={stage} /></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function FunnelPage() {
  const { data, error, loading } = useApi("/api/funnel");
  if (loading && !data) return <p>Measuring the funnel…</p>;
  if (error) return <p className="error">Could not read the funnel: {error}</p>;
  if (!data) return null;

  const funnel = data.funnel || { stages: [] };
  const sources = data.sources || {};
  const sourceLabels = { certstream: "CertStream", zone: "Zone files" };

  return (
    <section className="funnel-page">
      <h1>Pipeline — {data.day}</h1>

      <div className="throughput">
        <div className="throughput-card ok">
          <h3>Domains found today</h3>
          <p className="throughput-verdict">{num(funnel.entered)}</p>
          <dl>
            {Object.entries(sources).map(([key, count]) => (
              <div key={key}>
                <dt>{sourceLabels[key] || key}</dt>
                <dd>{num(count)}</dd>
              </div>
            ))}
          </dl>
        </div>
        <div className="throughput-card">
          <h3>Candidates</h3>
          <p className="throughput-verdict">{num(data.candidates)}</p>
          <dl><div><dt>reached the three buckets</dt></div></dl>
        </div>
        <div className="throughput-card">
          <h3>Claude said keep</h3>
          <p className="throughput-verdict">{num(data.kept)}</p>
        </div>
      </div>

      <Throughput windows={data.throughput} />

      <div className="funnel-legend">
        <span><i className="swatch seg-survived" /> Continued</span>
        <span><i className="swatch seg-dropped" /> Removed at this step</span>
      </div>

      {!data.funnel_live && (
        <p className="funnel-warn">
          No intake counted for {data.day} yet. This is "no data", not zero.
        </p>
      )}

      <FunnelTable funnel={funnel} />
      <p className="funnel-note">{funnel.note}</p>

      {data.biggest_losses?.length > 0 && (
        <p className="funnel-lede">
          Shaving most: {data.biggest_losses.map((l) => (
            `${l.label} (${num(l.dropped)}, ${pct(l.shave_of_top)} of the top)`
          )).join(" · ")}
        </p>
      )}
    </section>
  );
}

function App() {
  const [page, setPage] = useState(location.hash.slice(1) || "home");
  const [admin, setAdmin] = useState(false);
  const [loginOpen, setLoginOpen] = useState(false);
  useEffect(() => {
    const fn = () => setPage(location.hash.slice(1) || "home");
    addEventListener("hashchange", fn);
    return () => removeEventListener("hashchange", fn);
  }, []);
  useEffect(() => {
    api("/api/auth/status")
      .then((result) => setAdmin(result.admin))
      .catch(() => {});
  }, []);
  useEffect(() => {
    const loadedBundle = document.querySelector('script[type="module"][src]')
      ?.getAttribute("src") || "";
    const checkVersion = () => {
      fetch(`/api/version?t=${Date.now()}`, { cache: "no-store" })
        .then((response) => response.ok ? response.json() : null)
        .then((result) => {
          if (result?.bundle && loadedBundle && result.bundle !== loadedBundle) {
            window.location.reload();
          }
        })
        .catch(() => {});
    };
    const timer = window.setInterval(checkVersion, 30000);
    return () => window.clearInterval(timer);
  }, []);
  const logout = async () => {
    await api("/api/auth/logout", { method: "POST" });
    setAdmin(false);
  };
  const body = {
    home: <HomePage />,
    founders: <FounderSignalsPage />,
    certstream: <CertStreamPage />,
    funnel: <FunnelPage />,
    formd: <FormDPage />,
    csv: <SearchCorpusPage kind="ai" />,
    domains: <SearchCorpusPage kind="zone" />,
    health: <HealthPage />,
    claude: <ClaudeQueuePage />,
    landscape: <CompetitiveLandscapePage />,
  }[page] || <HomePage />;
  return (
    <AdminContext.Provider
      value={{ admin, requestLogin: () => setLoginOpen(true) }}
    >
      <ClaudeNotifier />
      <div className="app">
        <aside>
          <div className="brand">
            <img src={glasswingLogo} alt="Glasswing Ventures" />
            <span>SOURCING INTELLIGENCE</span>
          </div>
          <nav>
            {NAV.map(([id, label, Icon]) => (
              <a
                key={id}
                className={page === id ? "active" : ""}
                href={`#${id}`}
              >
                <Icon size={18} />
                {label}
              </a>
            ))}
          </nav>
          <button
            className="admin-button"
            onClick={admin ? logout : () => setLoginOpen(true)}
          >
            {admin ? <LogOut size={16} /> : <LogIn size={16} />}
            {admin ? "Log out admin" : "Admin login"}
          </button>
          <ApiStatus />
        </aside>
        <main>
          <img
            className="glasswing-watermark"
            src={glasswingMark}
            alt=""
            aria-hidden="true"
            loading="lazy"
            decoding="async"
          />
          <GlobalWorkerControls />
          <ErrorBoundary>{body}</ErrorBoundary>
        </main>
        {loginOpen && (
          <AdminLogin
            onClose={() => setLoginOpen(false)}
            onLogin={() => {
              setAdmin(true);
              setLoginOpen(false);
            }}
          />
        )}
      </div>
    </AdminContext.Provider>
  );
}
createRoot(document.getElementById("root")).render(<App />);
