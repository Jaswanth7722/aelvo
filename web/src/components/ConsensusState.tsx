import { useMemo } from "react";
import type { UIEvent, ConsensusState } from "../types";

interface ConsensusStateCardProps {
  events: UIEvent[];
}

export function ConsensusStateCard({ events }: ConsensusStateCardProps) {
  const state = useMemo<ConsensusState>(() => {
    const consensusEvents = events.filter(
      (e) => e.type === "consensus_formed"
    );

    if (consensusEvents.length === 0) {
      return {
        activeTopics: 0,
        resolvedTopics: 0,
        lastOutcome: "—",
        lastConfidence: 0,
        participants: [],
        positions: {},
      };
    }

    const latest = consensusEvents[consensusEvents.length - 1];
    const positions = (latest.data?.positions as Record<string, string>) || {};
    const participants = Object.keys(positions);

    // Count unique target_ids as topics
    const uniqueTargets = new Set(
      consensusEvents.map((e) => String(e.data?.target_id || ""))
    );
    // Assume latest events are resolved, earlier ones are the topics
    const resolvedTopics = uniqueTargets.size;

    return {
      activeTopics: Math.max(0, resolvedTopics - 1),
      resolvedTopics,
      lastOutcome: String(latest.data?.recommendation || latest.action || "—"),
      lastConfidence: Number(latest.data?.confidence || 0),
      participants,
      positions,
    };
  }, [events]);

  const posEntries = Object.entries(state.positions);
  const forCount = posEntries.filter(
    ([_, p]) => p.toLowerCase() === "for" || p.toLowerCase() === "yes" || p.toLowerCase() === "approve"
  ).length;
  const againstCount = posEntries.length - forCount;

  return (
    <div className="panel">
      <h3 className="text-xs text-ink-muted uppercase tracking-wider mb-4">Consensus State</h3>

      {/* Summary row */}
      <div className="grid grid-cols-3 gap-3 mb-4">
        <div className="text-center">
          <div className="text-xl font-bold text-accent-blue">{state.activeTopics}</div>
          <div className="text-xs text-ink-muted">Active</div>
        </div>
        <div className="text-center">
          <div className="text-xl font-bold text-accent-green">{state.resolvedTopics}</div>
          <div className="text-xs text-ink-muted">Resolved</div>
        </div>
        <div className="text-center">
          <div className="text-xl font-bold text-accent-purple">{state.participants.length}</div>
          <div className="text-xs text-ink-muted">Participants</div>
        </div>
      </div>

      {/* For/Against bar */}
      {posEntries.length > 0 && (
        <div className="mb-3">
          <div className="flex items-center justify-between text-xs text-ink-muted mb-1">
            <span>{forCount} For</span>
            <span>{againstCount} Against</span>
          </div>
          <div className="w-full h-1.5 bg-surface-border rounded-full overflow-hidden flex">
            {forCount > 0 && (
              <div
                className="h-full bg-accent-green transition-all duration-500"
                style={{ width: `${(forCount / posEntries.length) * 100}%` }}
              />
            )}
            {againstCount > 0 && (
              <div
                className="h-full bg-accent-red transition-all duration-500"
                style={{ width: `${(againstCount / posEntries.length) * 100}%` }}
              />
            )}
          </div>
        </div>
      )}

      {/* Latest outcome */}
      <div className="text-xs">
        <div className="text-ink-muted mb-1">Latest Outcome</div>
        <div className="flex items-center gap-2">
          <span className="text-ink font-medium truncate">
            {state.lastOutcome}
          </span>
          {state.lastConfidence > 0 && (
            <span
              className={`shrink-0 text-xs font-semibold ${
                state.lastConfidence >= 0.7
                  ? "text-accent-green"
                  : state.lastConfidence >= 0.4
                    ? "text-accent-amber"
                    : "text-accent-red"
              }`}
            >
              {Math.round(state.lastConfidence * 100)}%
            </span>
          )}
        </div>
      </div>

      {/* Position badges */}
      {posEntries.length > 0 && (
        <div className="mt-3 flex flex-wrap gap-1">
          {posEntries.slice(0, 8).map(([specialist, position]) => (
            <span
              key={specialist}
              className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${
                position.toLowerCase() === "for" || position.toLowerCase() === "approve"
                  ? "text-accent-green bg-accent-green/10"
                  : position.toLowerCase() === "against" || position.toLowerCase() === "reject"
                    ? "text-accent-red bg-accent-red/10"
                    : "text-ink-soft bg-surface-border/50"
              }`}
            >
              {specialist}: {position}
            </span>
          ))}
        </div>
      )}

      {posEntries.length === 0 && (
        <div className="text-xs text-ink-muted text-center py-2">No consensus activity yet</div>
      )}
    </div>
  );
}
