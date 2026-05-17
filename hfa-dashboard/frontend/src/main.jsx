import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

const API_BASE = import.meta.env.VITE_DASHBOARD_API_BASE || "http://127.0.0.1:8000";

async function fetchJson(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) throw new Error(`${path} failed: ${response.status}`);
  return response.json();
}

function StatCard({ label, value, hint }) {
  return <section className="card stat-card"><span className="muted">{label}</span><strong>{value}</strong><small>{hint}</small></section>;
}

function StatusBadge({ status }) {
  const normalized = String(status || "unknown").toLowerCase();
  return <span className={`badge badge-${normalized}`}>{status || "UNKNOWN"}</span>;
}

function KeyValueList({ rows }) {
  return <dl>{rows.map(([key, value]) => <React.Fragment key={key}><dt>{key}</dt><dd>{String(value ?? "—")}</dd></React.Fragment>)}</dl>;
}

function HeatmapTable({ rows }) {
  return (
    <section className="card">
      <div className="section-header"><h2>Top Risky Files</h2><span className="muted">{rows.length} files</span></div>
      <div className="table-wrap"><table>
        <thead><tr><th>Path</th><th>Risk</th><th>Allowed</th><th>Suspicious</th><th>Banned</th></tr></thead>
        <tbody>{rows.map((row) => <tr key={row.path}><td className="mono">{row.path}</td><td>{row.risk_score}</td><td>{row.allowed}</td><td>{row.suspicious}</td><td>{row.banned}</td></tr>)}</tbody>
      </table></div>
    </section>
  );
}

function FindingsList({ findings }) {
  return (
    <section className="card">
      <div className="section-header"><h2>Recent Findings</h2><span className="muted">read-only</span></div>
      <div className="findings">{findings.map((finding, index) => (
        <article className="finding" key={`${finding.path}-${finding.line}-${index}`}>
          <div><StatusBadge status={finding.severity} /> <span className="mono path">{finding.path}:{finding.line}</span></div>
          <p>{finding.reason}</p>
          <small className="mono">{finding.function} · {finding.category} · {finding.call}</small>
        </article>
      ))}</div>
    </section>
  );
}

function EvidencePanel({ title, children, note }) {
  return (
    <section className="card evidence-card">
      <div className="section-header"><h2>{title}</h2><span className="muted">read-only</span></div>
      <div className="evidence-body">{children}</div>
      {note ? <p className="note">{note}</p> : null}
    </section>
  );
}

function EventStreamPanel({ events }) {
  const items = events?.items || [];
  return (
    <section className="card">
      <div className="section-header"><h2>Event Stream</h2><span className="muted">{items.length} events</span></div>
      <div className="event-list">
        {items.length === 0 ? <p className="note">{events?.note || "No event data available."}</p> : null}
        {items.map((item, index) => (
          <article className="finding" key={`${item.key}-${index}`}>
            <span className="mono path">{item.key}</span>
            <pre>{JSON.stringify(item.event, null, 2)}</pre>
          </article>
        ))}
      </div>
    </section>
  );
}

function WorkerTelemetryPanel({ workers }) {
  const workerItems = workers?.workers || [];
  const effectItems = workers?.effects || [];
  return (
    <section className="card">
      <div className="section-header"><h2>Worker / Effect Telemetry</h2><span className="muted">observer</span></div>
      <div className="telemetry-grid">
        <StatCard label="Workers" value={workers?.counts?.workers ?? "—"} hint="read-only keys" />
        <StatCard label="Effects" value={workers?.counts?.effects ?? "—"} hint="ledger-like keys" />
        <StatCard label="Dead Letter" value={workers?.counts?.dead_letter ?? "—"} hint="dlq keys" />
      </div>
      <div className="mini-list">
        {[...workerItems.slice(0, 4), ...effectItems.slice(0, 4)].map((item) => (
          <article className="finding" key={item.key}>
            <span className="mono path">{item.key}</span>
            <pre>{JSON.stringify(item.payload, null, 2)}</pre>
          </article>
        ))}
      </div>
      <p className="note">{workers?.note}</p>
    </section>
  );
}

function Dashboard() {
  const [authority, setAuthority] = useState(null);
  const [heatmap, setHeatmap] = useState([]);
  const [findings, setFindings] = useState([]);
  const [replay, setReplay] = useState(null);
  const [quarantine, setQuarantine] = useState(null);
  const [artifacts, setArtifacts] = useState(null);
  const [events, setEvents] = useState(null);
  const [workers, setWorkers] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [a, h, f, r, q, av, ev, wk] = await Promise.all([
          fetchJson("/dashboard/authority"),
          fetchJson("/dashboard/authority/heatmap"),
          fetchJson("/dashboard/findings?limit=20"),
          fetchJson("/dashboard/replay"),
          fetchJson("/dashboard/quarantine"),
          fetchJson("/dashboard/artifacts?limit=8"),
          fetchJson("/dashboard/events?limit=12"),
          fetchJson("/dashboard/workers?limit=20"),
        ]);
        if (!cancelled) {
          setAuthority(a); setHeatmap(h.slice(0, 12)); setFindings(f.items || []);
          setReplay(r); setQuarantine(q); setArtifacts(av); setEvents(ev); setWorkers(wk); setError("");
        }
      } catch (err) {
        if (!cancelled) setError(err.message || String(err));
      }
    }
    load();
    const id = window.setInterval(load, 15000);
    return () => { cancelled = true; window.clearInterval(id); };
  }, []);

  const counts = authority?.counts || {};
  const topHeatmap = useMemo(() => heatmap || [], [heatmap]);

  return (
    <main>
      <header className="hero">
        <div>
          <p className="eyebrow">IRONCLAD / Toprak1</p>
          <h1>Read-only Command Center</h1>
          <p className="subtitle">Authority, replay, quarantine, artifact, event and worker evidence. No write or approval actions are exposed.</p>
        </div>
        <div className="status-panel"><span className="muted">Authority Status</span><StatusBadge status={authority?.authority_status || "LOADING"} /></div>
      </header>

      {error ? <section className="card error">Dashboard read failed: {error}</section> : null}

      <section className="grid stats">
        <StatCard label="Risk Score" value={authority?.risk_score ?? "—"} hint="lower is better" />
        <StatCard label="Banned" value={counts.banned ?? "—"} hint="merge-blocking risks" />
        <StatCard label="Suspicious" value={counts.suspicious ?? "—"} hint="visible backlog" />
        <StatCard label="Allowed" value={counts.allowed ?? "—"} hint="reviewed/authority paths" />
      </section>

      <section className="grid evidence-grid">
        <EvidencePanel title="Replay Integrity" note={replay?.note}>
          <KeyValueList rows={[["Replay compare", replay?.replay_compare_present], ["Smoke runner", replay?.smoke_runner_present], ["Last result", replay?.last_result]]} />
        </EvidencePanel>
        <EvidencePanel title="Quarantine Snapshot" note={quarantine?.note}>
          <KeyValueList rows={[["Pending", quarantine?.pending_count], ["Banned findings", quarantine?.known_banned_authority_findings], ["Actions", "disabled"]]} />
        </EvidencePanel>
        <EvidencePanel title="Artifact Vault" note={artifacts?.note}>
          <KeyValueList rows={[["Indexed files", artifacts?.count], ["Displayed", artifacts?.items?.length], ["Payload reads", "disabled"]]} />
        </EvidencePanel>
      </section>

      <section className="grid live-grid">
        <EventStreamPanel events={events} />
        <WorkerTelemetryPanel workers={workers} />
      </section>

      <section className="grid content-grid">
        <HeatmapTable rows={topHeatmap} />
        <FindingsList findings={findings} />
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<Dashboard />);
