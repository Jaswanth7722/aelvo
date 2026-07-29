# Execution - Command execution, tool operations & persistent sandbox
from core.execution.commands import AelvoKernel

from core.execution.tool_registry import (
    ToolExecutionRegistry,
    ToolSpec,
    ToolResult,
    CacheEntry,
    RetryPolicy,
    ToolCategory,
    ExecutionStrategy,
)
from core.execution.sandbox_session import (
    PersistentSandboxSession,
    SandboxSessionState,
    SandboxSessionCheckpoint,
    FileChangeRecord,
    SessionFileChange,
    SessionStatus,
)
from core.execution.experience_pipeline import (
    ExperienceLearningPipeline,
    ExperienceRecord,
    FailurePattern,
    RetrySuggestion,
    ErrorCategory,
    PatternSeverity,
)
