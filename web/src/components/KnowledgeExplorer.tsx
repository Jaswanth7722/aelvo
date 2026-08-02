import { useMemo, useState } from "react";
import type { UIEvent, KnowledgeItem, KnowledgeSortKey, SortDirection, ConsumptionLink } from "../types";

interface KnowledgeExplorerProps {
  events: UIEvent[];
}

/** Format timestamp for display */
function fmt(ts: number): string {
  const d = new Date(ts * 1000);
  return d.toLocaleDateString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

export function KnowledgeExplorer({ events }: KnowledgeExplorerProps) {
  const [search, setSearch] = useState("");
  const [typeFilter, setTypeFilter] = useState<string | null>(null);
  const [specFilter, setSpecFilter] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<string | null>(null);
  const [sortKey, setSortKey] = useState<KnowledgeSortKey>("timestamp");
  const [sortDir, setSortDir] = useState<SortDirection>("desc");
  const [selectedId, setSelectedId] = useState<string | null>(null);

  // Derive knowledge items from blackboard_publication events
  const { items, consumptionLinks, allTypes, allSpecialists, allStatuses } = useMemo(() => {
    const pubs = events.filter((e) => e.type === "blackboard_publication");
    const consumed = events.filter((e) => e.type === "finding_consumed");

    const itemMap = new Map<string, KnowledgeItem>();
    const links: ConsumptionLink[] = [];
    const types = new Set<string>();
    const specs = new Set<string>();
    const statuses = new Set<string>();

    // Build knowledge items from publications
    for (const pub of pubs) {
      const d = pub.data || {};
      const entryId = String(d.entry_id || d.id || `ev_${pub.timestamp}`);
      const specialist = String(d.specialist || pub.specialist || "");
      const entryType = String(d.entry_type || "finding");
      const verificationStatus = String(d.verification_status || "pending");
      const lifecycleStatus = String(d.lifecycle_status || "created");
      const confidence = Number(d.confidence || 0);
      const tags = Array.isArray(d.tags) ? d.tags : [];
      const affectedFiles = Array.isArray(d.affected_files) ? d.affected_files :
        typeof d.affected_files === "string" ? [d.affected_files] : [];

      if (specialist) specs.add(specialist.toUpperCase());
      types.add(entryType);
      statuses.add(verificationStatus);

      const existing = itemMap.get(entryId);
      if (existing) {
        // Update with latest data
        existing.summary = pub.action || existing.summary;
        existing.timestamp = Math.max(existing.timestamp, pub.timestamp);
        existing.confidence = confidence || existing.confidence;
        existing.verificationStatus = verificationStatus;
        existing.lifecycleStatus = lifecycleStatus;
        existing.challenged = Boolean(d.challenged) || existing.challenged;
        existing.challengeCount = Number(d.challenge_count || 0) || existing.challengeCount;
        existing.source = String(d.source || "") || existing.source;
      } else {
        itemMap.set(entryId, {
          id: entryId,
          specialist,
          entryType,
          summary: pub.action || "",
          tags,
          confidence,
          source: String(d.source || ""),
          verificationStatus,
          challenged: Boolean(d.challenged),
          challengeCount: Number(d.challenge_count || 0),
          lifecycleStatus,
          timestamp: pub.timestamp,
          affectedFiles,
          consumedBy: [],
          consumedTimestamps: [],
        });
      }
    }

    // Build consumption links from finding_consumed events
    for (const ev of consumed) {
      const d = ev.data || {};
      const entryId = String(d.entry_id || "");
      const consumer = String(d.consumer || d.specialist || "");
      const ct = ev.timestamp;
      const eType = String(d.entry_type || "");

      if (entryId && consumer) {
        links.push({ knowledgeId: entryId, consumer, timestamp: ct, entryType: eType });
        const item = itemMap.get(entryId);
        if (item) {
          item.consumedBy.push(consumer);
          item.consumedTimestamps.push(ct);
        }
      }
    }

    return {
      items: Array.from(itemMap.values()),
      consumptionLinks: links,
      allTypes: Array.from(types).sort(),
      allSpecialists: Array.from(specs).sort(),
      allStatuses: Array.from(statuses).sort(),
    };
  }, [events]);

  // Apply filters and sorting
  const filtered = useMemo(() => {
    let result = [...items];

    // Search
    if (search.trim()) {
      const q = search.toLowerCase();
      result = result.filter(
        (item) =>
          item.summary.toLowerCase().includes(q) ||
          item.specialist.toLowerCase().includes(q) ||
          item.entryType.toLowerCase().includes(q) ||
          item.source.toLowerCase().includes(q) ||
          item.id.toLowerCase().includes(q) ||
          item.verificationStatus.toLowerCase().includes(q) ||
          item.tags.some((t) => t.toLowerCase().includes(q))
      );
    }

    // Type filter
    if (typeFilter) {
      result = result.filter((item) => item.entryType === typeFilter);
    }

    // Specialist filter
    if (specFilter) {
      result = result.filter((item) => item.specialist.toUpperCase() === specFilter);
    }

    // Status filter
    if (statusFilter) {
      result = result.filter((item) => item.verificationStatus === statusFilter);
    }

    // Sort
    result.sort((a, b) => {
      let cmp = 0;
      switch (sortKey) {
        case "timestamp":
          cmp = a.timestamp - b.timestamp;
          break;
        case "confidence":
          cmp = a.confidence - b.confidence;
          break;
        case "specialist":
          cmp = a.specialist.localeCompare(b.specialist);
          break;
        case "entryType":
          cmp = a.entryType.localeCompare(b.entryType);
          break;
        case "verificationStatus":
          cmp = a.verificationStatus.localeCompare(b.verificationStatus);
          break;
        case "challengeCount":
          cmp = a.challengeCount - b.challengeCount;
          break;
      }
      return sortDir === "desc" ? -cmp : cmp;
    });

    return result;
  }, [items, search, typeFilter, specFilter, statusFilter, sortKey, sortDir]);

  const selected = selectedId ? items.find((i) => i.id === selectedId) : null;

  // Lineage: build a graph of who published what, who consumed it
  const lineage = useMemo(() => {
    if (!selected) return null;
    const consumed = consumptionLinks.filter((l) => l.knowledgeId === selected.id);
    const challengedBy = events.filter(
      (e) => e.type === "challenge_raised" && e.data?.entry_id === selected.id
    );
    return { consumed, challengedBy };
  }, [selected, consumptionLinks, events]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* ── Controls Bar ─────────────────────────────────── */}
      <div className="border-b border-surface-border px-6 py-3 space-y-3 shrink-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <h2 className="text-lg font-bold text-ink">Knowledge Explorer</h2>
            <span className="text-xs text-ink-muted">{filtered.length} of {items.length} items</span>
          </div>
        </div>

        {/* Filter row */}
        <div className="flex items-center gap-2 flex-wrap">
          {/* Search */}
          <div className="relative flex-1 min-w-[200px] max-w-sm">
            <span className="absolute left-3 top-1/2 -translate-y-1/2 text-ink-muted text-sm">🔍</span>
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search knowledge..."
              className="w-full bg-surface border border-surface-border rounded-lg pl-9 pr-3 py-1.5 text-sm text-ink placeholder-ink-muted focus:outline-none focus:border-accent-blue/50 transition-colors"
            />
          </div>

          {/* Type filter */}
          <select
            value={typeFilter || ""}
            onChange={(e) => setTypeFilter(e.target.value || null)}
            className="bg-surface border border-surface-border rounded-lg px-3 py-1.5 text-sm text-ink-soft focus:outline-none focus:border-accent-blue/50"
          >
            <option value="">All types</option>
            {allTypes.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>

          {/* Specialist filter */}
          <select
            value={specFilter || ""}
            onChange={(e) => setSpecFilter(e.target.value || null)}
            className="bg-surface border border-surface-border rounded-lg px-3 py-1.5 text-sm text-ink-soft focus:outline-none focus:border-accent-blue/50"
          >
            <option value="">All specialists</option>
            {allSpecialists.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>

          {/* Status filter */}
          <select
            value={statusFilter || ""}
            onChange={(e) => setStatusFilter(e.target.value || null)}
            className="bg-surface border border-surface-border rounded-lg px-3 py-1.5 text-sm text-ink-soft focus:outline-none focus:border-accent-blue/50"
          >
            <option value="">All statuses</option>
            {allStatuses.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>

          {/* Sort controls */}
          <div className="flex items-center gap-1 ml-auto">
            <select
              value={sortKey}
              onChange={(e) => setSortKey(e.target.value as KnowledgeSortKey)}
              className="bg-surface border border-surface-border rounded-lg px-3 py-1.5 text-sm text-ink-soft focus:outline-none focus:border-accent-blue/50"
            >
              <option value="timestamp">Time</option>
              <option value="confidence">Confidence</option>
              <option value="specialist">Specialist</option>
              <option value="entryType">Type</option>
              <option value="verificationStatus">Status</option>
              <option value="challengeCount">Challenges</option>
            </select>
            <button
              onClick={() => setSortDir(sortDir === "desc" ? "asc" : "desc")}
              className="bg-surface border border-surface-border rounded-lg px-2.5 py-1.5 text-sm text-ink-soft hover:text-ink transition-colors"
              title={sortDir === "desc" ? "Sort descending" : "Sort ascending"}
            >
              {sortDir === "desc" ? "↓" : "↑"}
            </button>
          </div>
        </div>
      </div>

      {/* ── Main content: list + detail panel ──────────────── */}
      <div className="flex-1 flex overflow-hidden">
        {/* Evidence list */}
        <div className={`${selected ? "w-1/2" : "flex-1"} overflow-y-auto border-r border-surface-border`}>
          {filtered.length === 0 ? (
            <div className="flex items-center justify-center h-full text-ink-muted text-sm">
              {search || typeFilter || specFilter || statusFilter
                ? "No knowledge matches your filters"
                : "No knowledge yet — waiting for blackboard publications"}
            </div>
          ) : (
            <div className="divide-y divide-surface-border">
              {filtered.map((item) => (
                <KnowledgeCard
                  key={item.id}
                  item={item}
                  isSelected={selectedId === item.id}
                  onSelect={() => setSelectedId(selectedId === item.id ? null : item.id)}
                />
              ))}
            </div>
          )}
        </div>

        {/* Detail panel (lineage view) */}
        {selected && lineage && (
          <div className="w-1/2 overflow-y-auto p-4 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-bold text-ink">Knowledge Lineage</h3>
              <button
                onClick={() => setSelectedId(null)}
                className="text-xs text-ink-muted hover:text-ink-soft transition-colors"
              >
                Close
              </button>
            </div>

            {/* Summary */}
            <div className="panel">
              <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs">
                <div className="text-ink-muted">ID</div>
                <div className="text-ink-soft font-mono">{selected.id.slice(0, 16)}…</div>
                <div className="text-ink-muted">Agent</div>
                <div className="text-ink-soft font-semibold">{selected.specialist}</div>
                <div className="text-ink-muted">Type</div>
                <div className="text-ink-soft">{selected.entryType}</div>
                <div className="text-ink-muted">Source</div>
                <div className="text-ink-soft">{selected.source || "—"}</div>
                <div className="text-ink-muted">Confidence</div>
                <div className="text-ink-soft">{Math.round(selected.confidence * 100)}%</div>
                <div className="text-ink-muted">Status</div>
                <VerifyBadge status={selected.verificationStatus} />
                <div className="text-ink-muted">Lifecycle</div>
                <div className="text-ink-soft">{selected.lifecycleStatus}</div>
                <div className="text-ink-muted">Challenges</div>
                <div className="text-ink-soft">{selected.challengeCount}</div>
                <div className="text-ink-muted">Tags</div>
                <div className="flex flex-wrap gap-1">
                  {selected.tags.length > 0
                    ? selected.tags.map((t, i) => (
                        <span key={i} className="text-[10px] text-ink-muted bg-surface-border/50 px-1.5 py-0.5 rounded">
                          {t}
                        </span>
                      ))
                    : <span className="text-ink-muted">—</span>}
                </div>
                <div className="text-ink-muted">Published</div>
                <div className="text-ink-soft">{fmt(selected.timestamp)}</div>
                {selected.affectedFiles && selected.affectedFiles.length > 0 && (
                  <>
                    <div className="text-ink-muted">Files</div>
                    <div className="flex flex-wrap gap-1">
                      {selected.affectedFiles.map((f, i) => (
                        <span key={i} className="text-[10px] text-ink-soft bg-surface-border/50 px-1.5 py-0.5 rounded font-mono">
                          {f}
                        </span>
                      ))}
                    </div>
                  </>
                )}
              </div>
              {selected.summary && (
                <div className="mt-3 pt-3 border-t border-surface-border">
                  <div className="text-xs text-ink-muted mb-1">Summary</div>
                  <p className="text-sm text-ink-soft leading-relaxed">{selected.summary}</p>
                </div>
              )}
            </div>

            {/* Consumption trail */}
            <div>
              <h4 className="text-xs text-ink-muted uppercase tracking-wider mb-2">
                Consumption Trail ({lineage.consumed.length})
              </h4>
              {lineage.consumed.length === 0 ? (
                <p className="text-xs text-ink-muted">No consumers yet</p>
              ) : (
                <div className="space-y-1.5">
                  {lineage.consumed.map((link, i) => (
                    <div key={i} className="flex items-center gap-2 text-xs bg-surface-alt/50 px-3 py-2 rounded-lg border border-surface-border">
                      <span className="text-accent-blue font-semibold">{link.consumer}</span>
                      <span className="text-ink-muted">consumed</span>
                      <span className="text-ink-muted font-mono">{fmt(link.timestamp)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Challenges */}
            <div>
              <h4 className="text-xs text-ink-muted uppercase tracking-wider mb-2">
                Challenges ({lineage.challengedBy.length})
              </h4>
              {lineage.challengedBy.length === 0 ? (
                <p className="text-xs text-ink-muted">No challenges raised</p>
              ) : (
                <div className="space-y-1.5">
                  {lineage.challengedBy.map((ev, i) => (
                    <div key={i} className="text-xs bg-accent-red/5 px-3 py-2 rounded-lg border border-accent-red/20">
                      <div className="flex items-center gap-2">
                        <span className="text-accent-red font-semibold">{ev.specialist}</span>
                        <span className="text-ink-muted">{fmt(ev.timestamp)}</span>
                      </div>
                      <p className="text-ink-soft mt-1">{ev.action}</p>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Knowledge Card ──────────────────────────────────────────────── */

interface KnowledgeCardProps {
  item: KnowledgeItem;
  isSelected: boolean;
  onSelect: () => void;
}

function KnowledgeCard({ item, isSelected, onSelect }: KnowledgeCardProps) {
  const specColor = (
    {
      ORACLE: "#8c5cff",
      FORGE: "#00e38c",
      SENTINEL: "#ff5c7a",
      ARCHITECT: "#3b82f6",
      HERALD: "#39c8ff",
      TERMINUS: "#f7b731",
      CONSENSUS: "#19f5a5",
    } as Record<string, string>
  )[item.specialist.toUpperCase()] || "#52627f";

  return (
    <div
      className={`px-4 py-3 cursor-pointer transition-colors duration-150 hover:bg-surface-border/20 ${
        isSelected ? "bg-accent-blue/5" : ""
      }`}
      onClick={onSelect}
    >
      {/* Top row: specialist + type + status + ID */}
      <div className="flex items-center gap-2 mb-1">
        <span className="text-[10px] font-mono text-ink-muted">{item.id.slice(0, 10)}</span>
        <span className="text-xs font-bold" style={{ color: specColor }}>
          {item.specialist}
        </span>
        <span
          className="text-[10px] px-1.5 py-0.5 rounded font-medium"
          style={{ color: specColor, backgroundColor: `${specColor}15` }}
        >
          {item.entryType}
        </span>
        <VerifyBadge status={item.verificationStatus} />
        {item.challenged && (
          <span className="text-[10px] text-accent-red font-semibold">⚠ CHALLENGED</span>
        )}
        <span className="text-[10px] text-ink-muted ml-auto">{fmt(item.timestamp)}</span>
      </div>

      {/* Summary */}
      <p className="text-sm text-ink-soft leading-relaxed line-clamp-2">{item.summary}</p>

      {/* Bottom row: metadata */}
      <div className="flex items-center gap-3 mt-1.5 flex-wrap">
        <span className="text-[10px] text-ink-muted">
          Confidence: {Math.round(item.confidence * 100)}%
        </span>
        {item.challengeCount > 0 && (
          <span className="text-[10px] text-accent-red">⚠ {item.challengeCount}</span>
        )}
        {item.consumedBy.length > 0 && (
          <span className="text-[10px] text-accent-blue">
            Consumed by {item.consumedBy.join(", ")}
          </span>
        )}
        {item.affectedFiles && item.affectedFiles.length > 0 && (
          <span className="text-[10px] text-ink-muted">
            Files: {item.affectedFiles.slice(0, 3).join(", ")}{item.affectedFiles.length > 3 ? ` +${item.affectedFiles.length - 3}` : ""}
          </span>
        )}
        {item.source && (
          <span className="text-[10px] text-ink-muted">via {item.source}</span>
        )}
      </div>
    </div>
  );
}

/* ── Verification Badge ──────────────────────────────────────────── */

function VerifyBadge({ status }: { status: string }) {
  const s = status.toLowerCase();
  if (s === "verified" || s === "passed") {
    return <span className="text-[10px] text-accent-green font-semibold">✅ Verified</span>;
  }
  if (s === "challenged" || s === "failed") {
    return <span className="text-[10px] text-accent-red font-semibold">❌ {status}</span>;
  }
  return <span className="text-[10px] text-ink-muted">◌ {status}</span>;
}
