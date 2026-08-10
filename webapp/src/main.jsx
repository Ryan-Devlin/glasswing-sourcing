import React, { createContext, useContext, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  Bot,
  CheckCircle2,
  Database,
  EyeOff,
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
import { formatCount as fmt, pageCount, plainSnippet } from "./domain";

const NAV = [
  ["home", "Home", Home],
  ["certstream", "CertStream Scraper", Activity],
  ["csv", "CSV Scraper", Database],
  ["domains", "Domain Crawler (.com, .app)", Globe2],
  ["health", "System Health", HeartPulse],
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
async function api(path, options = {}) {
  const r = await fetch(path, {
    headers: {
      ...(options.body instanceof FormData
        ? {}
        : { "Content-Type": "application/json" }),
      ...(options.headers || {}),
    },
    ...options,
  });
  if (!r.ok) throw new Error((await r.text()) || `Request failed ${r.status}`);
  return r.json();
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
  onHide,
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
            {(onLabel || onHide || onClaude) && <th>Actions</th>}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={row.domain || i}>
              {columns.map((c) => (
                <td key={c.key}>
                  {c.render ? c.render(row) : String(row[c.key] ?? "")}
                </td>
              ))}
              {(onLabel || onHide || onClaude) && (
                <td className="actions">
                  {onLabel && (
                    <>
                      <button
                        className="yes"
                        onClick={() => onLabel(row, "yes")}
                      >
                        Yes
                      </button>
                      <button className="no" onClick={() => onLabel(row, "no")}>
                        No
                      </button>
                    </>
                  )}
                  {onHide && (
                    <button
                      className="hide"
                      title="Hide from review tables"
                      onClick={() => onHide(row)}
                    >
                      <EyeOff size={13} />
                      Hide
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
          ))}
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
        <span
          className={`pill claude-${r.claude_decision.toLowerCase()}`}
          title={r.claude_reasoning || ""}
        >
          {r.claude_decision}{" "}
          {r.claude_score !== "" ? `· ${r.claude_score}` : ""}
        </span>
      ) : (
        <span className="muted">Not reviewed</span>
      ),
  },
  {
    key: "thesis_pillar",
    label: "Category",
    render: (r) => String(r.thesis_pillar || "").replaceAll("_", " "),
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
  if (overview.loading) return <Loading />;
  if (overview.error) return <ErrorBox>{overview.error}</ErrorBox>;
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
          <span className={`status ${String(d.overall).toLowerCase()}`}>
            {d.overall}
          </span>
        </div>
        <div className="metrics">
          <Metric
            label="Companies tracked"
            value={fmt(d.counts.classifications)}
          />
          <Metric label="Investable" value={fmt(d.buckets.investable)} />
          <Metric label="Needs review" value={fmt(d.buckets.review)} />
          <Metric label="Hard denied" value={fmt(d.buckets.denied)} />
        </div>
      </section>
      <LabelsPanel />
    </>
  );
}

function LabelsPanel() {
  const { admin, requestLogin } = useAdmin();
  const [label, setLabel] = useState("yes");
  const [refresh, setRefresh] = useState(0);
  const result = useApi(`/api/labels?label=${label}`, [label, refresh]);
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
          onChange={setLabel}
          tabs={[
            ["yes", "YES"],
            ["no", "NO"],
          ]}
        />
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
    </section>
  );
}

function CompaniesTable() {
  const { admin, requestLogin } = useAdmin();
  const [bucket, setBucket] = useState("investable");
  const [page, setPage] = useState(1);
  const [refresh, setRefresh] = useState(0);
  const [label, setLabel] = useState(null);
  const [toast, setToast] = useState("");
  const [sort, setSort] = useState({ key: "updated_at", direction: "desc" });
  const [claudeBusy, setClaudeBusy] = useState("");
  const basket = bucket === "yes" || bucket === "no";
  const query = basket
    ? `/api/labels?label=${bucket}`
    : `/api/classifications?bucket=${bucket}&page=${page}&page_size=100&sort=${encodeURIComponent(apiSort(sort))}`;
  const result = useApi(query, [bucket, page, refresh]);
  const save = (row, label) => setLabel({ domain: row.domain, label });
  const hide = async (row) => {
    await api("/api/hidden", {
      method: "POST",
      body: JSON.stringify({ domains: [row.domain], hidden: true }),
    });
    setToast(`${row.domain} hidden from review tables`);
    setRefresh((x) => x + 1);
    setTimeout(() => setToast(""), 2500);
  };
  const sendClaude = async (row) => {
    if (!admin) return requestLogin();
    setClaudeBusy(row.domain);
    try {
      const response = await api(
        `/api/claude/review/${encodeURIComponent(row.domain)}`,
        { method: "POST" },
      );
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
      setToast(`Claude could not review ${row.domain}: ${error.message}`);
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
        />
      ) : (
        <>
          <Table
            rows={result.data.rows.filter((r) => !r.hidden)}
            columns={companyCols}
            onLabel={admin ? save : (_, label) => requestLogin(label)}
            onHide={admin ? hide : requestLogin}
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
    </section>
  );
}
function RunFilters() {
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
      <div className="extension-grid">
        {extensions.map((extension) => (
          <label key={extension}>
            <input
              type="checkbox"
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
            min="0"
            max="100"
            value={form.minimum_score}
            onChange={(e) =>
              setForm({ ...form, minimum_score: Number(e.target.value) })
            }
          />
        </label>
      </div>
      <div className="filter-actions">
        <button className="primary" onClick={save}>
          {admin ? "Save filters" : "Admin login to edit"}
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
  const status = useApi("/api/certstream/status", [tick]);
  const claude = useApi("/api/claude/status", [tick]);
  useEffect(() => {
    const id = setInterval(() => setTick((x) => x + 1), 30000);
    return () => clearInterval(id);
  }, []);
  const s = status.data || {};
  const control = async (desired) => {
    setControlBusy(true);
    try {
      await api("/api/runtime/pipeline", {
        method: "POST",
        body: JSON.stringify({ desired }),
      });
      setTick((x) => x + 1);
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
        <h2>
          {s.running ? "Running" : "Stopped"} ·{" "}
          {String(s.stage || "waiting").replaceAll("_", " ")}
        </h2>
        <div>
          <button
            disabled={controlBusy || s.running}
            onClick={() => (admin ? control(true) : requestLogin())}
          >
            <Play size={16} />
            Start
          </button>
          <button
            disabled={controlBusy || !s.running}
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
      </div>
      {claude.data?.warning && (
        <p className="notice">Claude: {claude.data.warning}</p>
      )}
      {s.last_domain_received && (
        <p className="notice">
          Latest domain: <strong>{s.last_domain_received}</strong>
        </p>
      )}
      <RunFilters />
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
  const [toast, setToast] = useState("");
  const basket = bucket === "yes" || bucket === "no";
  const result = useApi(
    basket
      ? `/api/labels?label=${bucket}`
      : `/api/corpus/${kind}?bucket=${bucket}&page=${page}&page_size=100&sort=${encodeURIComponent(apiSort(sort))}`,
    [bucket, page, refresh],
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
        />
      ) : (
        <>
          <Table
            rows={result.data.rows}
            columns={companyCols}
            onLabel={(row, label) =>
              admin ? setLabel({ domain: row.domain, label }) : requestLogin()
            }
            onClaude={async (row) => {
              if (!admin) return requestLogin();
              setClaudeBusy(row.domain);
              try {
                const response = await api(
                  `/api/claude/review/${encodeURIComponent(row.domain)}`,
                  { method: "POST" },
                );
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
                setToast(
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
      () => setMetricsTick((value) => value + 1),
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
      render: (r) => plainSnippet(r.snippet),
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
  const health = useApi(`/api/health?full=${full}`, [full, tick]);
  if (health.loading) return <Loading />;
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
        <button onClick={() => setFull(true)}>Run full checks</button>
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
                <span>Desired: {row.desired ? "running" : "paused"}</span>
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

function GlobalWorkerControls() {
  const { admin } = useAdmin();
  const [tick, setTick] = useState(0);
  const [busy, setBusy] = useState(false);
  const runtime = useApi("/api/runtime", [tick]);
  useEffect(() => {
    if (!admin) return;
    const timer = setInterval(() => setTick((value) => value + 1), 30000);
    return () => clearInterval(timer);
  }, [admin]);
  if (!admin) return null;
  const services = runtime.data?.services || {};
  const values = Object.values(services);
  const allRunning = values.length > 0 && values.every((row) => row.desired);
  const allPaused = values.length > 0 && values.every((row) => !row.desired);
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
            ? "All workers running"
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
  }[page] || <HomePage />;
  return (
    <AdminContext.Provider
      value={{ admin, requestLogin: () => setLoginOpen(true) }}
    >
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
