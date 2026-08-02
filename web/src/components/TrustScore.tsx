import { useMemo } from "react";
import type { UIEvent, TrustState } from "../types";

interface TrustScoreProps {
  events: UIEvent[];
}

export function TrustScore({ events }: TrustScoreProps) {
  const state = useMemo<TrustState>(() => {
    // Collect all blackboard publications with confidence data
    const findings = events.filter(
      (e) => e.type === "blackboard_publication" && e.data?.confidence != null
    );

    if (findings.length === 0) {
      return {
        averageConfidence: 0,
        totalFindings: 0,
        verifiedCount: 0,
        challengedCount: 0,
        pendingCount: 0,
        recentScore: 0,
      };
    }

    let totalConfidence = 0;
    let verified = 0;
    let challenged = 0;
    let pending = 0;

    for (const f of findings) {
      const conf = Number(f.data.confidence) || 0;
      totalConfidence += conf;

      // Use the boolean challenged flag as the primary indicator
      // (backend sets this when a challenge exists)
      if (Boolean(f.data.challenged)) {
        challenged++;
      } else {
        const vStatus = String(f.data.verification_status || "pending");
        if (vStatus === "verified") verified++;
        else if (vStatus === "challenged") challenged++;
        else pending++;
      }
    }

    const avgConf = totalConfidence / findings.length;

    // Recent score = weighted combination of avg confidence and verified ratio
    const verifiedRatio = findings.length > 0 ? verified / findings.length : 0;
    const recentScore = avgConf * 0.6 + verifiedRatio * 0.4;

    return {
      averageConfidence: Math.round(avgConf * 100),
      totalFindings: findings.length,
      verifiedCount: verified,
      challengedCount: challenged,
      pendingCount: pending,
      recentScore: Math.round(recentScore * 100),
    };
  }, [events]);

  const scoreColor =
    state.recentScore >= 80
      ? "text-accent-green"
      : state.recentScore >= 50
        ? "text-accent-amber"
        : "text-accent-red";

  const barColor =
    state.recentScore >= 80
      ? "bg-accent-green"
      : state.recentScore >= 50
        ? "bg-accent-amber"
        : "bg-accent-red";

  return (
    <div className="panel">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-xs text-ink-muted uppercase tracking-wider">Trust Score</h3>
        <span className={`text-2xl font-bold ${scoreColor}`}>
          {state.recentScore}
          <span className="text-xs text-ink-muted">%</span>
        </span>
      </div>

      {/* Score bar */}
      <div className="w-full h-2 bg-surface-border rounded-full overflow-hidden mb-4">
        <div
          className={`h-full rounded-full ${barColor} transition-all duration-700 ease-out`}
          style={{ width: `${state.recentScore}%` }}
        />
      </div>

      {/* Metrics grid */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs">
        <div className="text-ink-muted">Avg Confidence</div>
        <div className="text-ink text-right font-medium">{state.averageConfidence}%</div>

        <div className="text-ink-muted">Total Findings</div>
        <div className="text-ink text-right font-medium">{state.totalFindings}</div>

        <div className="text-accent-green">Verified</div>
        <div className="text-ink text-right font-medium">{state.verifiedCount}</div>

        <div className="text-accent-red">Challenged</div>
        <div className="text-ink text-right font-medium">{state.challengedCount}</div>

        <div className="text-ink-muted">Pending</div>
        <div className="text-ink text-right font-medium">{state.pendingCount}</div>
      </div>
    </div>
  );
}
