/**
 * AELVO unified web theme — single source of truth for colors.
 *
 * Palette philosophy:
 *   Warm  → brand orange / gold / cream (primary actions, highlights)
 *   Cool  → purple / deep violet / cyan  (accents, info, secondary actions)
 *   Minimal → snow white surfaces + neutral ink grays
 *
 * Every dashboard, timeline, and badge should draw from here instead of
 * scattering raw hex values.
 */

export const PALETTE = {
  orange: "#FF9F45",
  gold: "#FFC98A",
  cream: "#FFF7EC",
  snow: "#FFFFFF",
  purple: "#8B5CF6",
  deep: "#7C3AED",
  cyan: "#0891B2",
  teal: "#0D9488",
  green: "#16A34A",
  red: "#E11D48",
  amber: "#F59E0B",
  neutral: "#6B7280",
  muted: "#9CA3AF",
} as const;

/** Specialist agent colors — canonical map shared by chat, dashboards, timeline, explorer. */
export const AGENT_COLORS: Record<string, string> = {
  HERMES: PALETTE.cyan,
  ARCHITECT: PALETTE.deep,
  ORACLE: PALETTE.purple,
  FORGE: PALETTE.green,
  SENTINEL: PALETTE.red,
  TERMINUS: PALETTE.amber,
  HERALD: PALETTE.orange,
  CONSENSUS: PALETTE.teal,
};

export const AGENT_ICONS: Record<string, string> = {
  HERMES: "◉",
  ARCHITECT: "◈",
  ORACLE: "◆",
  FORGE: "⚙",
  SENTINEL: "🛡",
  TERMINUS: "▶",
  HERALD: "★",
  CONSENSUS: "↻",
};

export function agentColor(name: string): string {
  return AGENT_COLORS[name?.toUpperCase()] || PALETTE.neutral;
}

/** Semantic status colors (success / warning / danger / info / accent / neutral). */
export const STATUS = {
  ok: PALETTE.green,
  warn: PALETTE.amber,
  err: PALETTE.red,
  info: PALETTE.cyan,
  accent: PALETTE.purple,
  neutral: PALETTE.neutral,
  muted: PALETTE.muted,
} as const;

/** Decision-outcome → color (used by Consensus & Governance dashboards). */
export const OUTCOME_COLORS: Record<string, string> = {
  approve: PALETTE.green,
  approved: PALETTE.green,
  rejected: PALETTE.red,
  reject: PALETTE.red,
  denied: PALETTE.red,
  escalated: PALETTE.amber,
  escalate: PALETTE.amber,
  revision: PALETTE.cyan,
  replanned: PALETTE.purple,
  replan: PALETTE.purple,
  override: PALETTE.deep,
  overridden: PALETTE.deep,
};

/** Event type → color map used by health breakdowns and feeds. */
export const EVENT_COLORS: Record<string, string> = {
  blackboard_publication: PALETTE.purple,
  finding_consumed: PALETTE.teal,
  challenge_raised: PALETTE.red,
  consensus_formed: PALETTE.teal,
  architect_decision: PALETTE.deep,
  execution_started: PALETTE.amber,
  execution_completed: PALETTE.green,
  report_generated: PALETTE.orange,
  recovery_initiated: PALETTE.deep,
  recovery_completed: PALETTE.green,
  recovery_failed: PALETTE.red,
  node_transition: PALETTE.purple,
  graph_started: PALETTE.amber,
  graph_completed: PALETTE.green,
  task_created: PALETTE.neutral,
  task_assigned: PALETTE.purple,
  task_completed: PALETTE.green,
  task_failed: PALETTE.red,
  system_online: PALETTE.green,
  verification_started: PALETTE.amber,
  verification_passed: PALETTE.green,
  verification_failed: PALETTE.red,
  verification_running: PALETTE.neutral,
  task_board_transition: PALETTE.neutral,
};

/** Task board column → color. */
export const TASK_STATUS_COLORS: Record<string, string> = {
  pending: PALETTE.neutral,
  assigned: PALETTE.purple,
  active: PALETTE.deep,
  review: PALETTE.amber,
  blocked: PALETTE.red,
  completed: PALETTE.green,
  complete: PALETTE.green,
};
