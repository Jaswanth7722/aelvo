/** Matches UIEventType from the Python backend */
export type UIEventType = string;

/** A UIEvent from the Python bridge, serialised as JSON */
export interface UIEvent {
  type: UIEventType;
  source: string;
  specialist: string;
  action: string;
  data: Record<string, unknown>;
  timestamp: number;
  icon: string;
  color: string;
}

/** Connection status to the WebSocket bridge */
export type ConnectionStatus = "connecting" | "connected" | "disconnected" | "error";

/** System overview snapshot */
export interface SystemOverview {
  provider: string;
  model: string;
  agents: number;
  uptime: number;
  goal: string;
  task: string;
  progress: number;
}

/** ── Phase 6: Dashboard State Types ──────────────────────────── */

export interface TrustState {
  averageConfidence: number;
  totalFindings: number;
  verifiedCount: number;
  challengedCount: number;
  pendingCount: number;
  recentScore: number;
}

export interface ConsensusState {
  activeTopics: number;
  resolvedTopics: number;
  lastOutcome: string;
  lastConfidence: number;
  participants: string[];
  positions: Record<string, string>;
}

export interface RecoveryState {
  totalEvents: number;
  succeeded: number;
  failed: number;
  successRate: number;
  recentActions: string[];
}

export interface VerificationState {
  totalChecks: number;
  passed: number;
  failed: number;
  running: number;
  passRate: number;
}

export interface TaskSummary {
  total: number;
  pending: number;
  active: number;
  completed: number;
  failed: number;
  blocked: number;
}

/** ── Phase 7: Task Board Types ──────────────────────────────── */

export type TaskStatus =
  | "pending"
  | "assigned"
  | "active"
  | "review"
  | "blocked"
  | "completed"
  | "failed";

export interface TaskBoardItem {
  id: string;
  taskId: string;
  taskType: string;
  status: TaskStatus;
  specialist: string;
  reason: string;
  sessionId: string;
  timestamp: number;
  lastUpdated: number;
}

export interface KanbanColumn {
  key: TaskStatus;
  label: string;
  icon: string;
  color: string;
}

/** ── Phase 9: Knowledge Explorer Types ────────────────────────── */

export interface KnowledgeItem {
  id: string;
  specialist: string;
  entryType: string;
  summary: string;
  tags: string[];
  confidence: number;
  source: string;
  verificationStatus: string;
  challenged: boolean;
  challengeCount: number;
  lifecycleStatus: string;
  timestamp: number;
  affectedFiles: string[];
  consumedBy: string[];
  consumedTimestamps: number[];
}

export type KnowledgeSortKey =
  | "timestamp"
  | "confidence"
  | "specialist"
  | "entryType"
  | "verificationStatus"
  | "challengeCount";

export type SortDirection = "asc" | "desc";

export interface ConsumptionLink {
  knowledgeId: string;
  consumer: string;
  timestamp: number;
  entryType: string;
}

/** ── Phase 11: Agent Dashboard Types ─────────────────────────── */

export interface AgentState {
  name: string;
  label: string;
  color: string;
  icon: string;
  status: "active" | "idle" | "thinking" | "acting";
  currentTask: string;
  confidence: number;
  successRate: number;
  contributionScore: number;
  metrics: {
    totalEvents: number;
    publications: number;
    consumptions: number;
    verifications: number;
    decisions: number;
    executions: number;
    reports: number;
  };
  recentActions: AgentAction[];
}

export interface AgentAction {
  type: string;
  summary: string;
  timestamp: number;
  color: string;
  icon: string;
}

export interface AgentConfig {
  key: string;
  label: string;
  color: string;
  icon: string;
}

/** ── Phase 10: Consensus Dashboard Types ─────────────────────── */

export interface ConsensusRecord {
  id: string;
  targetId: string;
  recommendation: string;
  confidence: number;
  method: string;
  positions: Record<string, string>;
  timestamp: number;
}

export interface DecisionRecord {
  id: string;
  outcome: string;
  targetType: string;
  targetId: string;
  reason: string;
  conditions: string[];
  assignedTo: string;
  overriddenRec: string;
  timestamp: number;
}

export interface ChallengeRecord {
  id: string;
  entryId: string;
  challenger: string;
  claim: string;
  evidence: string;
  timestamp: number;
}

/** ── Phase 12: System Health Types ────────────────────────── */

export type HealthStatus = "healthy" | "degraded" | "unhealthy" | "unknown";

export interface AgentLiveness {
  name: string;
  label: string;
  color: string;
  icon: string;
  lastSeen: number;
  status: "active" | "idle" | "unknown";
  eventCount: number;
  recentActions: string[];
}

export interface RecoveryEvent {
  id: string;
  type: string;
  specialist: string;
  action: string;
  outcome: "initiated" | "completed" | "failed";
  timestamp: number;
}

export interface NodeState {
  nodeId: string;
  state: string;
  previousState: string;
  timestamp: number;
}

export interface EventTypeBreakdown {
  type: string;
  count: number;
  icon: string;
  color: string;
}

/** ── Phase 13: Governance Dashboard Types ──────────────────── */

export interface GovernanceEvaluation {
  id: string;
  decisionId: string;
  outcome: "approve" | "reject" | "escalate" | "replan" | "override";
  targetType: string;
  targetId: string;
  reason: string;
  assignedTo: string;
  conditions: string[];
  timestamp: number;
}

export interface SecurityAudit {
  id: string;
  type: string;
  specialist: string;
  action: string;
  status: "passed" | "failed" | "running" | "pending";
  details: string;
  timestamp: number;
}

export interface PolicyDefinition {
  id: string;
  name: string;
  description: string;
  effect: "allow" | "deny" | "require_approval" | "log_only";
  scope: string;
  specialists: string[];
  actionTypes: string[];
  priority: number;
  enabled: boolean;
}

export interface AuditEntry {
  id: string;
  type: string;
  actor: string;
  action: string;
  outcome: string;
  subsystem: string;
  reason: string;
  timestamp: number;
}

/** ── Phase 14: Monitoring Dashboard Types ────────────────────── */

export interface MonitorAlert {
  id: string;
  severity: "critical" | "error" | "warning" | "info";
  title: string;
  message: string;
  subsystem: string;
  source: string;
  timestamp: number;
  acknowledged: boolean;
}

export interface MonitorRule {
  id: string;
  name: string;
  description: string;
  subsystem: string;
  severity: "critical" | "error" | "warning" | "info";
  metricName: string;
  thresholdMin: number | null;
  thresholdMax: number | null;
  enabled: boolean;
  cooldownSeconds: number;
}

export interface MetricSnapshot {
  name: string;
  count: number;
  avg: number | null;
  min: number | null;
  max: number | null;
  latest: number | null;
}

export interface EventRateBucket {
  label: string;
  count: number;
  periodStart: number;
}

export interface SubsystemHealthSummary {
  name: string;
  status: "healthy" | "degraded" | "unhealthy" | "unknown";
  checksPassing: number;
  checksFailing: number;
  totalChecks: number;
  activeAlerts: number;
  description: string;
}

/** ── Phase 17: Chat Workspace Types ────────────────────────── */

/** A single message in the chat */
export interface ChatMessage {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  timestamp: number;
  /** For assistant messages — which specialists contributed */
  agentSteps?: AgentStep[];
  /** For assistant messages — verification summary */
  verificationSummary?: VerificationStepStatus[];
  /** Active phases the message went through */
  phases?: ChatPhase[];
  /** Whether message is still being streamed */
  streaming?: boolean;
  /** Token stream so far (for live display) */
  streamedContent?: string;
}

/** A step taken by a specialist during processing */
export interface AgentStep {
  agent: string;
  action: string;
  status: "active" | "completed" | "failed" | "pending";
  duration?: number;
  details?: string;
  timestamp: number;
}

/** Verification check status */
export interface VerificationStepStatus {
  check: string;
  status: "passed" | "failed" | "running" | "pending";
  details?: string;
}

/** Phase of the AELVO pipeline */
export interface ChatPhase {
  name: string;
  specialist: string;
  status: "pending" | "active" | "completed" | "failed";
  timestamp: number;
}

/** ── Phase 16: Admin Settings Types ─────────────────────────── */

export interface WSConfig {
  url: string;
  reconnectDelay: number;
  maxEvents: number;
}

/** ── CLI-style File Access Types ───────────────────────────── */

export interface FsEntry {
  name: string;
  type: "dir" | "file";
  size: number;
  modified: number;
}

export interface FsListing {
  cwd: string;
  root: string;
  entries: FsEntry[];
}

export interface FsReadResult {
  path: string;
  content: string;
  truncated: boolean;
  encoding: string;
  size: number;
}

export interface FsWorkspaceResult {
  success: boolean;
  root?: string;
  message?: string;
  error?: string;
}

/** ── Provider Setup Types ──────────────────────────────────── */

export interface ProviderInfo {
  key: string;
  name: string;
  env_key: string;
  default_model: string;
  sdk: string;
  local: boolean;
  has_key: boolean;
  base_url: string;
}
