# types.py - Highly Typed Memory Entry Schemas for AELVO OMEGA

import time
import hashlib
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field

# --- BASE SCHEMA ---
class MemoryEntry(BaseModel):
    id: str = Field(default_factory=lambda: "")
    type: str
    content: str
    importance: float = 0.5
    timestamp_unix: float = Field(default_factory=time.time)
    usage_count: int = 0
    project: str = "default"
    source_specialist: str = "orchestrator"

    def __init__(self, **data):
        super().__init__(**data)
        if not self.id:
            raw = f"{self.type}_{self.timestamp_unix}_{self.content[:50]}"
            self.id = hashlib.sha256(raw.encode("utf-8")).hexdigest()

# --- SPECIALIST SPECIFIC SCHEMAS ---

class CodePatternEntry(MemoryEntry):
    type: str = "code_pattern"
    file_path: str
    language: str
    pattern_type: str  # class, function, error_handler, test_fixture, architectural
    symbol_signature: str

class UserPreferenceEntry(MemoryEntry):
    type: str = "user_preference"
    preference_category: str  # communication_style, expertise_domain, workflow_mode, vocabulary, avoid
    confidence: float = 0.5  # Float representing user feedback confidence

class ResearchFindingEntry(MemoryEntry):
    type: str = "research_finding"
    sources: List[str] = Field(default_factory=list)
    credibility_tier: int = 4  # Tiers 1 (RFC, official docs) to 4 (blogs)
    publication_dates: List[str] = Field(default_factory=list)
    contradictions_found: List[str] = Field(default_factory=list)
    wiki_entry: str

class SecurityRuleEntry(MemoryEntry):
    type: str = "security_rule"
    vulnerability_type: str  # sql_injection, path_traversal, xss, ssrf, secret_exposure
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW
    file_path: str
    pattern_to_avoid: str
    remediated_example: str

class SystemDecisionEntry(MemoryEntry):
    type: str = "system_decision"
    decision: str
    rationale: str
    alternatives_considered: List[str] = Field(default_factory=list)
    tradeoffs: str
    adr_id: str

class DevOpsPatternEntry(MemoryEntry):
    type: str = "devops_pattern"
    command_sequence: List[str] = Field(default_factory=list)
    environment_requirements: List[str] = Field(default_factory=list)
    failure_mode: str = ""
    recovery_steps: str = ""

class ArchitectureMapEntry(MemoryEntry):
    type: str = "architecture_map"
    diagram_type: str = "mermaid"

class SessionSummaryEntry(MemoryEntry):
    type: str = "session_summary"
    turn_count: int

class VoluntaryMemoryEntry(MemoryEntry):
    type: str = "voluntary"
    topic: str = ""
    category: str = "general"

class SemanticMemoryEntry(MemoryEntry):
    type: str = "semantic"
    tags: List[str] = Field(default_factory=list)
    confidence: float = 0.5

