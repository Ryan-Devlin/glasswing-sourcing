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
  Pause,
  Play,
  RefreshCw,
  Search,
  Trash2,
  Upload,
} from "lucide-react";
import "./styles.css";
import "./polish.css";
import { dealAgeLabel, formatCount as fmt, pageCount, plainSnippet } from "./domain";

const NAV = [
  ["home", "Home", Home],
  ["certstream", "CertStream Scraper", Activity],
  ["csv", "CSV Scraper", Database],
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
async function api(path, options = {}) {
  const method = String(options.method || "GET").toUpperCase();
  const request = async () => {
    const r = await fetch(path, {
      headers: {
        ...(options.body instanceof FormData
          ? {}
          : { "Content-Type": "application/json" }),
        ...(options.headers || {}),
      },
      ...options,
    });
    if (!r.ok) {
      const text = await r.text();
      const error = new Error(text || `Request failed ${r.status}`);
      error.status = r.status;
      try { error.payload = JSON.parse(text); } catch {}
      throw error;
    }
    return r.json();
  };
  if (method !== "GET") return request();
  if (inflightGets.has(path)) return inflightGets.get(path);
  const pending = request().finally(() => inflightGets.delete(path));
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
  const [state, set] = useState({ data: null, error: "", loading: true });
  useEffect(() => {
    let live = true;
    set((s) => ({ ...s, loading: true, error: "" }));
    api(path)
      .then((data) => live && set({ data, error: "", loading: false }))
      .catch(
        (e) => live && set({ data: null, error: e.message, loading: false }),
      );
    return () => {
      live = false;
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
function Loading() {
  return (
    <div className="loading">
      <RefreshCw size={18} />
      Loading…
    </div>
  );
}
function ErrorBox({ children }) {
  return <div className="error">{children}</div>;
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
                      <button className="no" disabled={bucketed} title={bucketed ? `Already bucketed ${bucketLabel}` : "Move to NO"} onClick={() => onLabel(row, "no")}>
                        No
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
                        Boolean(row.claude_decision)
                      }
                      title={
                        row.claude_decision
                          ? `Reviewed: ${row.claude_decision}`
                          : "Send this company through Claude diligence"
                      }
                      onClick={() => onClaude(row)}
                    >
                      <Bot size={13} />
                      {claudeBusy === row.domain
                        ? "Reviewing…"
                        : row.claude_decision
                          ? "Reviewed"
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
  const [rating, setRating] = useState(target.label === "yes" ? 85 : 10);
  const [saving, setSaving] = useState(false);
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
        <div className="modal-actions">
          <button onClick={onClose}>Cancel</button>
          <button
            className="primary"
            disabled={saving}
            onClick={async () => {
              setSaving(true);
              await api("/api/labels", {
                method: "POST",
                body: JSON.stringify({
                  domain: target.domain,
                  label: target.label,
                  rating: Number(rating),
                }),
              });
              onSaved();
            }}
          >
            Save decision
          </button>
        </div>
      </div>
    </div>
  );
}

function HomePage() {
  const overview = useApi("/api/overview", []);
  const [q, setQ] = useState("");
  const [rows, setRows] = useState([]);
  const [busy, setBusy] = useState(false);
  const d = overview.data;
  return (
    <>
      <header>
        <p className="eyebrow">Glasswing Ventures</p>
        <h1>Sourcing command center</h1>
        <p>
          Search every tracked company and crawler corpus without blocking the
          page.
        </p>
      </header>
      <GoodDeals />
      <ClaudeTopPicks />
      <form
        className="searchbar"
        onSubmit={async (e) => {
          e.preventDefault();
          if (!q.trim()) return;
          setBusy(true);
          setRows((await api(`/api/search?q=${encodeURIComponent(q)}`)).rows);
          setBusy(false);
        }}
      >
        <Search />
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search company, market, product, or domain"
        />
        <button>Search</button>
      </form>
      {busy ? (
        <Loading />
      ) : (
        rows.length > 0 && (
          <Table
            rows={rows}
            columns={[
              { key: "company", label: "Company" },
              {
                key: "domain",
                label: "Domain",
                render: (r) => (
                  <a href={r.url} target="_blank">
                    {r.domain}
                  </a>
                ),
              },
              { key: "source", label: "Source" },
              { key: "score", label: "Score" },
              { key: "description", label: "Description" },
            ]}
          />
        )
      )}
      <section>
        <div className="section-title">
          <h2>System overview</h2>
          {d && (
            <span className={`status ${String(d.overall).toLowerCase()}`}>
              {d.overall}
            </span>
          )}
        </div>
        {overview.error ? (
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
      <CompaniesTable />
    </>
  );
}

function GoodDeals() {
  const { admin, requestLogin } = useAdmin();
  const [refresh, setRefresh] = useState(0);
  const [claudeOverride, setClaudeOverride] = useState(null);
  const [claudeBusy, setClaudeBusy] = useState("");
  const [message, setMessage] = useState("");
  const deals = useApi(
    "/api/classifications?bucket=investable&page=1&page_size=25&sort=Highest%20score",
    [refresh],
  );
  if (deals.loading) return <Loading />;
  if (deals.error) return <ErrorBox>{deals.error}</ErrorBox>;
  const rows = (deals.data?.rows || []).filter((row) => !row.hidden).slice(0, 6);
  if (!rows.length) return null;
  const reject = async (row) => {
    if (!admin) return requestLogin();
    await api("/api/labels", {
      method: "POST",
      body: JSON.stringify({
        domain: row.domain, label: "no", rating: Number(row.fit_score || 0),
        reason: "manual_reject",
      }),
    });
    setRefresh((value) => value + 1);
  };
  const sendClaude = async (row, overrideDailyLimit = false) => {
    if (!admin) return requestLogin();
    setClaudeBusy(row.domain);
    try {
      await requestClaudeReview(row.domain, overrideDailyLimit);
      setClaudeOverride(null);
      setMessage(`${row.domain} queued for Claude diligence`);
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
            <button className="card-dismiss" title="Reject to Hard Denied and NO" onClick={() => reject(row)}>Reject</button>
            <span className="deal-age" title={row.classified_at || row.updated_at}>{dealAgeLabel(row.classified_at || row.updated_at)}</span>
            <div className="top-pick-head">
              <span className="score-ring">{row.fit_score}<small>/100</small></span>
              <span className="pill">Investable</span>
            </div>
            <h3>{row.company_name || row.domain}</h3>
            <a href={row.final_url || `https://${row.domain}`} target="_blank">{row.domain}</a>
            {row.claude_decision ? (
              <div className="deal-claude-review">
                <ClaudeReviewBadge row={row} prefix="Claude " />
              </div>
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
            <p className="clamp-text" title={row.description}>{row.description || row.routing_reason}</p>
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

function ClaudeTopPicks() {
  const { admin, requestLogin } = useAdmin();
  const [refresh, setRefresh] = useState(0);
  const picks = useApi("/api/claude/top?limit=6", [refresh]);
  if (picks.loading) return <Loading />;
  if (picks.error || !picks.data?.rows?.length) return null;
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
        {picks.data.rows.map((row) => {
          const reason = shortClaudeReason(
            row.reasoning || row.product_summary,
          );
          return (
            <article className="top-pick" key={row.domain}>
              <span className="deal-age" title={row.classified_at || row.updated_at}>{dealAgeLabel(row.classified_at || row.updated_at)}</span>
              <button className="card-dismiss" title="Reject to Hard Denied and NO" onClick={async () => {
                if (!admin) return requestLogin();
                await api("/api/labels", {
                  method: "POST",
                  body: JSON.stringify({
                    domain: row.domain, label: "no", rating: Number(row.fit_score || 0),
                    reason: "manual_reject",
                  }),
                });
                setRefresh((value) => value + 1);
              }}>Reject</button>
              <div className="top-pick-head">
                <span className="score-ring">
                  {row.fit_score}
                  <small>/100</small>
                </span>
                <span className="pill claude-keep">Claude KEEP</span>
              </div>
              <h3>{row.company_name || row.domain}</h3>
              <a
                href={row.final_url || `https://${row.domain}`}
                target="_blank"
              >
                {row.domain}
              </a>
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
  const [q, setQ] = useState("");
  const result = useApi(
    `/api/labels?label=${label}&page=${page}&page_size=100&q=${encodeURIComponent(q)}`,
    [label, page, q, refresh],
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
        <input
          value={q}
          onChange={(e) => {
            setQ(e.target.value);
            setPage(1);
          }}
          placeholder="Search basket domains"
        />
        <a className="button-link" href={`/api/export/labels/${label}`}>
          Export CSV
        </a>
      </div>
      {result.loading ? (
        <Loading />
      ) : result.error ? (
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

function CompaniesTable() {
  const { admin, requestLogin } = useAdmin();
  const [bucket, setBucket] = useState("investable");
  const [scope, setScope] = useState("current");
  const [page, setPage] = useState(1);
  const [refresh, setRefresh] = useState(0);
  const [label, setLabel] = useState(null);
  const [toast, setToast] = useState("");
  const [sort, setSort] = useState({ key: "updated_at", direction: "desc" });
  const [claudeBusy, setClaudeBusy] = useState("");
  const [claudeOverride, setClaudeOverride] = useState(null);
  const basket = bucket === "yes" || bucket === "no";
  const query = basket
    ? `/api/labels?label=${bucket}&scope=${scope}&page=${page}&page_size=100`
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
  const reject = async (row) => {
    await api("/api/labels", {
      method: "POST",
      body: JSON.stringify({
        domain: row.domain, label: "no", rating: Number(row.fit_score || row.rating || 0),
        reason: "manual_reject",
      }),
    });
    setToast(`${row.domain} moved to Hard Denied and NO`);
    setRefresh((x) => x + 1);
    setTimeout(() => setToast(""), 2500);
  };
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
          href={`${basket ? `/api/export/labels/${bucket}` : `/api/export/classifications/${bucket}`}?scope=${scope}`}
        >
          Export current bucket CSV
        </a>
      </div>
      {result.loading ? (
        <Loading />
      ) : result.error ? (
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
  useEffect(() => {
    if (result.data) setForm(result.data);
  }, [result.data]);
  if (result.loading || !form) return <Loading />;
  if (result.error) return <ErrorBox>{result.error}</ErrorBox>;
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
              onChange={(event) =>
                setForm({
                  ...form,
                  selected_extensions: event.target.checked
                    ? [...form.selected_extensions, extension]
                    : form.selected_extensions.filter(
                        (value) => value !== extension,
                      ),
                })
              }
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
              setForm({
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
              setForm({
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
              setForm({ ...form, minimum_score: Number(e.target.value) })
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
                setForm({ ...form, claude_daily_limit: Number(e.target.value) })
              }
            />
            <input
              type="number"
              disabled={running}
              min="1"
              max="5000"
              value={form.claude_daily_limit ?? 400}
              onChange={(e) =>
                setForm({
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
  const claude = useApi("/api/claude/status", [tick]);
  useEffect(() => {
    const id = setInterval(() => {
      if (document.visibilityState === "visible") setTick((x) => x + 1);
    }, 30000);
    return () => clearInterval(id);
  }, []);
  const s = status.data || {};
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
        <Metric
          label="New domains"
          value={fmt(s.new_domains_received || s.domains_received)}
        />
        <Metric label="Ignored repeats" value={fmt(s.repeat_events_ignored)} />
        <Metric label="Claude calls" value={fmt(s.claude_calls)} />
        <Metric label="Failed" value={fmt(s.failed_this_run)} />
        <Metric label="Reviewed by Claude" value={fmt(claude.data?.reviewed)} />
        <Metric label="Claude KEEP" value={fmt(claude.data?.kept)} />
        <Metric label="Claude REJECT" value={fmt(claude.data?.rejected)} />
        <Metric label="Awaiting new rubric" value={fmt(claude.data?.awaiting_rescore)} />
      </div>
      {claude.data?.warning && (
        <p className="notice">Claude: {claude.data.warning}</p>
      )}
      <RunFilters running={Boolean(desiredRunning)} />
      <CompaniesTable />
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
  const reject = async (row) => {
    if (!admin) return requestLogin();
    await api("/api/labels", {
      method: "POST",
      body: JSON.stringify({
        domain: row.domain, label: "no", rating: Number(row.fit_score || row.rating || 0),
        reason: "manual_reject",
      }),
    });
    setToast(`${row.domain} moved to Hard Denied and NO`);
    setRefresh((value) => value + 1);
  };
  const basket = bucket === "yes" || bucket === "no";
  const result = useApi(
    basket
      ? `/api/labels?label=${bucket}&page=${page}&page_size=100`
      : `/api/corpus/${kind}?bucket=${bucket}&page=${page}&page_size=100&sort=${encodeURIComponent(apiSort(sort))}`,
    [bucket, page, refresh, sort.key, sort.direction],
  );
  return (
    <section>
      <Toast>{toast}</Toast>
      <div className="table-toolbar table-heading">
        <div>
          <h2>Crawled company buckets</h2>
          <p>Only records sourced from this crawler are included.</p>
        </div>
      </div>
      <BucketTabs
        value={bucket}
        onChange={(value) => {
          setBucket(value);
          setPage(1);
        }}
      />
      {result.loading ? (
        <Loading />
      ) : result.error ? (
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
          <Pager page={page} setPage={setPage} total={result.data.total} />
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

function SearchCorpusPage({ kind }) {
  const { admin, requestLogin } = useAdmin();
  const isAi = kind === "ai";
  const [view, setView] = useState("search");
  const [q, setQ] = useState("");
  const [active, setActive] = useState("");
  const [page, setPage] = useState(1);
  const [metricsTick, setMetricsTick] = useState(0);
  const metrics = useApi(`/api/corpus/${kind}/stats`, [kind, metricsTick]);
  const result = useApi(
    active
      ? `/api/${kind}/search?q=${encodeURIComponent(active)}&page=${page}&page_size=100`
      : "/api/healthz",
    [active, page],
  );
  const [upload, setUpload] = useState(null);
  const [uploading, setUploading] = useState(false);
  const rows = Array.isArray(result.data?.rows) ? result.data.rows : [];
  useEffect(() => {
    if (!isAi || !metrics.data?.pending) return;
    const timer = setInterval(
      () => {
        if (document.visibilityState === "visible") {
          setMetricsTick((value) => value + 1);
        }
      },
      15000,
    );
    return () => clearInterval(timer);
  }, [isAi, metrics.data?.pending]);
  const cols = [
    {
      key: "domain",
      label: "Domain",
      render: (r) => (
        <a href={r.final_url || `https://${r.domain}`} target="_blank">
          {r.domain}
        </a>
      ),
    },
    { key: "title", label: "Title" },
    { key: "description", label: "Description" },
    {
      key: "snippet",
      label: "Match",
      render: (r) => (
        <span className="clamp-text" title={plainSnippet(r.snippet)}>
          {plainSnippet(r.snippet)}
        </span>
      ),
    },
    { key: "word_count", label: "Words" },
    { key: "crawled_at", label: "Crawled" },
  ];
  return (
    <>
      <header>
        <p className="eyebrow">Crawler corpus</p>
        <h1>{isAi ? "CSV Scraper" : "Domain Crawler (.com, .app)"}</h1>
        <p>
          {isAi
            ? "Upload or search crawled domains from any source."
            : "Search retained website content without loading the zone corpus into the browser."}
        </p>
      </header>
      {metrics.loading ? (
        <Loading />
      ) : metrics.error ? (
        <ErrorBox>Metrics unavailable: {metrics.error}</ErrorBox>
      ) : (
        <div className="metrics corpus-metrics">
          <Metric
            label="Domains available"
            value={fmt(metrics.data.available)}
          />
          <Metric label="Waiting to scrape" value={fmt(metrics.data.pending)} />
          <Metric label="Crawled" value={fmt(metrics.data.crawled)} />
          <Metric
            label="Searchable pages"
            value={fmt(metrics.data.searchable)}
          />
          <Metric label="Failed" value={fmt(metrics.data.failed)} />
        </div>
      )}
      <div className="segmented">
        <button
          className={view === "search" ? "active" : ""}
          onClick={() => setView("search")}
        >
          Search corpus
        </button>
        <button
          className={view === "companies" ? "active" : ""}
          onClick={() => setView("companies")}
        >
          Company buckets
        </button>
      </div>
      {view === "companies" ? (
        <CorpusCompanies kind={kind} />
      ) : (
        <>
          {isAi && (
            <section className="upload">
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
            </section>
          )}
          <form
            className="searchbar"
            onSubmit={(e) => {
              e.preventDefault();
              setPage(1);
              setActive(q.trim());
            }}
          >
            <Search />
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Search indexed website content"
            />
            <button>Search</button>
          </form>
          {!active ? (
            <div className="empty">
              Enter a technology, market, product, or customer phrase.
            </div>
          ) : result.loading ? (
            <Loading />
          ) : result.error ? (
            <ErrorBox>{result.error}</ErrorBox>
          ) : (
            <>
              <Table rows={rows} columns={cols} />
              <Pager
                page={page}
                setPage={setPage}
                total={page * 100 + (rows.length === 100 ? 100 : 0)}
              />
            </>
          )}
        </>
      )}
    </>
  );
}

function HealthPage() {
  const { admin, requestLogin } = useAdmin();
  const [full, setFull] = useState(false);
  const [tick, setTick] = useState(0);
  const health = useApi(`/api/health?full=${full}&refresh=${tick}`, [full, tick]);
  if (health.loading && !health.data) return <Loading />;
  if (health.error) return <ErrorBox>{health.error}</ErrorBox>;
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
      {activity.loading ? (
        <Loading />
      ) : activity.error ? (
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
      {queue.loading ? (
        <Loading />
      ) : queue.error ? (
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
    row.desired && /running|screening|crawling|importing|starting/.test(
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
        <label className="wide"><span>Company website *</span><input name="website" type="text" inputMode="url" placeholder="https://company.com" required /></label>
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
  const logout = async () => {
    await api("/api/auth/logout", { method: "POST" });
    setAdmin(false);
  };
  const body = {
    home: <HomePage />,
    certstream: <CertStreamPage />,
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
            <div className="mark">G</div>
            <div>
              <strong>Glasswing</strong>
              <span>VENTURES</span>
            </div>
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
