import React, { createContext, useContext, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
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
function Table({ rows, columns, onLabel, onHide }) {
  if (!rows?.length) return <div className="empty">No records found.</div>;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key}>{c.label}</th>
            ))}
            {(onLabel || onHide) && <th>Actions</th>}
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
              {(onLabel || onHide) && (
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
  { key: "company_name", label: "Company" },
  {
    key: "domain",
    label: "Domain",
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
  { key: "fit_score", label: "Score" },
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
  const basket = bucket === "yes" || bucket === "no";
  const query = basket
    ? `/api/labels?label=${bucket}`
    : `/api/classifications?bucket=${bucket}&page=${page}&page_size=100&sort=${encodeURIComponent("Recently classified")}`;
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
function CertStreamPage() {
  const { admin, requestLogin } = useAdmin();
  const [tick, setTick] = useState(0);
  const [controlBusy, setControlBusy] = useState(false);
  const status = useApi("/api/certstream/status", [tick]);
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
      </div>
      {s.last_domain_received && (
        <p className="notice">
          Latest domain: <strong>{s.last_domain_received}</strong>
        </p>
      )}
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
  const basket = bucket === "yes" || bucket === "no";
  const result = useApi(
    basket
      ? `/api/labels?label=${bucket}`
      : `/api/corpus/${kind}?bucket=${bucket}&page=${page}&page_size=100`,
    [bucket, page, refresh],
  );
  return (
    <section>
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
  const result = useApi(
    active
      ? `/api/${kind}/search?q=${encodeURIComponent(active)}&page=${page}&page_size=100`
      : "/api/healthz",
    [active, page],
  );
  const [upload, setUpload] = useState("");
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
                    const body = new FormData();
                    body.append("file", f);
                    const r = await api("/api/ai/upload", {
                      method: "POST",
                      body,
                    });
                    setUpload(`Added ${r.added} of ${r.received} domains`);
                  }}
                />
                {!admin && (
                  <button onClick={requestLogin}>Admin login to upload</button>
                )}
                {upload && <p>{upload}</p>}
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
              <Table rows={result.data.rows} columns={cols} />
              <Pager
                page={page}
                setPage={setPage}
                total={page * 100 + (result.data.rows.length === 100 ? 100 : 0)}
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
    </>
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
