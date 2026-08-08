# settings.py - Central Configuration and Threshold Constants for AELVO OMEGA

import os
from pathlib import Path

# --- DIRECTORY CONSTANTS ---
BASE_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_BASE = BASE_DIR / "workspace"
DEFAULT_BACKUP_DIR = BASE_DIR / "backups"
GLOBAL_DB_PATH = BASE_DIR / "global_memory.db"
GLOBAL_ANCHOR_PATH = BASE_DIR / "global_anchor.md"


def get_data_dir() -> Path:
    """Resolve the user-level data directory for global AELVO state.

    ``AELVO_DATA_DIR`` (when set) relocates the global metadata DB, anchor,
    credential vault, logs and LLM cache out of the package directory — this
    is what lets an npm-installed ``Aelvo`` write state to ``~/.aelvo``
    instead of a read-only ``node_modules`` tree. When unset, everything
    stays in the repo (BASE_DIR) exactly as before.
    """
    env = os.environ.get("AELVO_DATA_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return BASE_DIR


DATA_DIR = get_data_dir()
# Global state lives in the data dir so npm/global installs stay writable.
if str(DATA_DIR) != str(BASE_DIR):
    GLOBAL_DB_PATH = DATA_DIR / "global_memory.db"
    GLOBAL_ANCHOR_PATH = DATA_DIR / "global_anchor.md"

# Ensure directories exist
os.makedirs(WORKSPACE_BASE, exist_ok=True)
os.makedirs(DEFAULT_BACKUP_DIR, exist_ok=True)

# --- AGENT GOVERNANCE & EXECUTION LIMITS ---
ACTION_BUDGET_PER_TURN = 30
CIRCUIT_BREAKER_LIMIT = 3
HISTORY_WINDOW_SIZE = 12
MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5MB
LOCK_TIMEOUT_SECONDS = 5.0
COMMAND_CHAR_LIMIT = 2000
DEFAULT_TOOL_TIMEOUT_SECONDS = 30

# --- SPECIALIST REGISTRY & ACTIVATION ---
ACTIVATION_THRESHOLD_DEFAULT = 0.6
CACHE_TREE_EXPIRY_SECONDS = 300.0  # 5 minutes

# --- HYBRID MEMORY & LIFE-CYCLE DECAY ---
# Starting Importance weights by Memory Category
IMPORTANCE_VOLUNTARY = 0.6
IMPORTANCE_SEMANTIC = 0.8
IMPORTANCE_CODE_PATTERN = 0.7
IMPORTANCE_ERROR_RECOVERY = 0.8
IMPORTANCE_DEVOPS_PATTERN = 0.75
IMPORTANCE_USER_PREFERENCE = 0.9
IMPORTANCE_RESEARCH_FINDING = 0.75
IMPORTANCE_SECURITY_RULE = 1.0
IMPORTANCE_SYSTEM_DECISION = 0.8
IMPORTANCE_ARCHITECTURE_MAP = 0.75
IMPORTANCE_CONVENTION = 0.7
IMPORTANCE_CI_KNOWLEDGE = 0.7
IMPORTANCE_SESSION_SUMMARY = 0.7
IMPORTANCE_CONSENSUS_RECORD = 0.8
IMPORTANCE_COLLABORATION_PATTERN = 0.75
IMPORTANCE_SPECIALIST_EFFECTIVENESS = 0.7
IMPORTANCE_REVIEW_PATTERN = 0.85

# Memory Decay Constants
MEMORY_DECAY_RATE = 0.995        # Multiplier applied to importance every turn
MEMORY_RECENCY_HALF_LIFE = 172800.0  # 48 hours in seconds
MEMORY_NOISE_FLOOR = 0.15         # Items below this score are filtered as noise

# Factual Conflict Resolution (Semantic Similarity Thresholds)
CONFLICT_SIMILARITY_DUPLICATE = 0.95  # Similarity above this skips insert (redundant)
CONFLICT_SIMILARITY_OVERRIDE = 0.85   # Similarity above this prunes older stale entries

# --- DEVELOPER TOOL PATHS & COMMANDS ---
RUFF_BINARY = "ruff"
MYPY_BINARY = "mypy"
PYTEST_BINARY = "pytest"
TSC_BINARY = "tsc"
ESLINT_BINARY = "eslint"
PRETTIER_BINARY = "prettier"
VITEST_BINARY = "vitest"
CARGO_BINARY = "cargo"
GO_BINARY = "go"
GIT_BINARY = "git"

# Tool invocation timeouts (seconds)
LINTER_TIMEOUT_SECONDS = 30
FORMATTER_TIMEOUT_SECONDS = 30
TYPE_CHECKER_TIMEOUT_SECONDS = 60
TEST_RUNNER_TIMEOUT_SECONDS = 120
SYMBOL_GRAPH_TIMEOUT_SECONDS = 30
GIT_TOOL_TIMEOUT_SECONDS = 15
BASH_EXEC_DEFAULT_TIMEOUT = 30

# --- FORGE SPECIALIST CONFIG ---
FORGE_ACTION_BUDGET = ACTION_BUDGET_PER_TURN
FORGE_ACTIVATION_THRESHOLD = 0.6
FORGE_MAX_MEMORY_HITS_IN_PROMPT = 5
FORGE_MAX_TREE_CHARS_IN_PROMPT = 2000
FORGE_PROJECT_TREE_CACHE_TTL = 300.0  # 5 minutes, seconds
FORGE_NOISE_FLOOR = MEMORY_NOISE_FLOOR
FORGE_MIN_PATTERN_LINES = 5
FORGE_MAX_PATTERN_DESCRIPTION_CHARS = 200

# --- ORACLE SPECIALIST CONFIG ---
ORACLE_SUBQUERY_COUNT = 3
ORACLE_MAX_SOURCES = 6

# --- SESSION & ARCHIVE CONFIG ---
SESSION_SUMMARY_BOUNDARY = 50

# --- LONG-HORIZON PLANNING CONFIG ---
LHP_ENABLED = True                          # Master switch for the planning system
LHP_MAX_ACTIVE_OBJECTIVES = 5               # Maximum simultaneously active Strategic Objectives
LHP_MAX_ACTIVE_MILESTONES = 10              # Maximum simultaneously active Milestones
LHP_CONFIDENCE_FLOOR = 0.20                 # If milestone confidence drops below this, escalate
LHP_VERIFICATION_FAILURE_PENALTY = 0.10    # Confidence penalty per verification failure
LHP_CAPABILITY_DISCOVERY_BOOST = 0.08      # Confidence boost per capability discovery
LHP_RESOURCE_CONSTRAINT_PENALTY = 0.15     # Confidence penalty per resource constraint
LHP_DEBT_HIGH_THRESHOLD = 0.60             # Debt score above this → HIGH risk
LHP_DEBT_MEDIUM_THRESHOLD = 0.30           # Debt score above this → MEDIUM risk
LHP_CRITIQUE_ESCALATION_RUNS = 3           # Consecutive runs before a defect is escalated
IMPORTANCE_STRATEGIC_PLAN = 0.85           # Default importance for strategic plan nodes
IMPORTANCE_SESSION_BOUNDARY = 0.90         # Default importance for session boundary records
