# repository_memory.py - Repository Memory System for Repository Intelligence
# Layer 13: Maintains long-term repository awareness and learning

import time
import logging
import sqlite3
import json
from typing import List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

from repo_intelligence.types import PerformanceMetrics
from repo_intelligence.types_extended import (
    ModificationRecord, ModificationPattern, Hotspot, ComponentBreakage,
    FragileComponent, BreakagePattern, ArchitecturalDecision, DecisionEvolution,
    QueryContext, KnownRisk, RiskStatus, TimeWindow
)

log = logging.getLogger("aelvo.repo_intelligence.repository_memory")


class RepositoryMemorySystem:
    """Main memory system that coordinates all memory components"""
    
    def __init__(self, db_path: str = "repository_memory.db"):
        """
        Initialize the repository memory system.
        
        Args:
            db_path: Path to SQLite database for persistent storage
        """
        self.db_path = db_path
        
        # Ensure parent directory exists before SQLite connect
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.modification_tracker = HistoricalModificationTracker(db_path)
        self.fragile_registry = FragileComponentRegistry(db_path)
        self.decision_recorder = ArchitecturalDecisionRecorder(db_path)
        self.risk_registry = KnownRiskRegistry(db_path)
        self.metrics: List[PerformanceMetrics] = []
        
        # Initialize database schema
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the database schema for all memory components"""
        self.modification_tracker._initialize_schema()
        self.fragile_registry._initialize_schema()
        self.decision_recorder._initialize_schema()
        self.risk_registry._initialize_schema()
        log.info(f"Repository memory system initialized at {self.db_path}")
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        """Record a performance metric"""
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    # Delegate methods to individual components
    def record_modification(self, modification: ModificationRecord) -> None:
        """Record a modification and its context"""
        self.modification_tracker.record_modification(modification)
    
    def get_hotspots(self, time_window: Optional[TimeWindow] = None) -> List[Hotspot]:
        """Identify frequently modified areas (hotspots)"""
        return self.modification_tracker.get_hotspots(time_window)
    
    def analyze_modification_patterns(self) -> List[ModificationPattern]:
        """Analyze patterns in historical modifications"""
        return self.modification_tracker.analyze_modification_patterns()
    
    def register_breakage(self, breakage: ComponentBreakage) -> None:
        """Record a component breakage event"""
        self.fragile_registry.register_breakage(breakage)
    
    def get_fragile_components(self) -> List[FragileComponent]:
        """Returns components ranked by fragility"""
        return self.fragile_registry.get_fragile_components()
    
    def record_decision(self, decision: ArchitecturalDecision) -> None:
        """Record an architectural decision with context"""
        self.decision_recorder.record_decision(decision)
    
    def retrieve_relevant_decisions(self, context: QueryContext) -> List[ArchitecturalDecision]:
        """Retrieves architectural decisions relevant to a context"""
        return self.decision_recorder.retrieve_relevant_decisions(context)
    
    def register_risk(self, risk: KnownRisk) -> None:
        """Register a known risk"""
        self.risk_registry.register_risk(risk)
    
    def update_risk_status(self, risk_id: str, status: RiskStatus) -> None:
        """Update the status of a known risk"""
        self.risk_registry.update_risk_status(risk_id, status)
    
    def get_active_risks(self, component: Optional[str] = None) -> List[KnownRisk]:
        """Retrieves active risks, optionally filtered by component"""
        return self.risk_registry.get_active_risks(component)
    
    def get_metrics(self) -> List[PerformanceMetrics]:
        """Return all recorded metrics"""
        return self.metrics.copy()


class HistoricalModificationTracker:
    """Tracks historical modifications and their outcomes"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def _initialize_schema(self):
        """Initialize the database schema for modification tracking"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS modifications (
                    modification_id TEXT PRIMARY KEY,
                    timestamp DATETIME,
                    modified_files TEXT,
                    modified_symbols TEXT,
                    modification_type TEXT,
                    specialist TEXT,
                    success BOOLEAN,
                    issues TEXT,
                    task_context TEXT
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_modifications_timestamp 
                ON modifications(timestamp)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_modifications_component 
                ON modifications(modified_symbols)
            ''')
            conn.commit()
    
    def record_modification(self, modification: ModificationRecord) -> None:
        """Record a modification and its context"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO modifications 
                (modification_id, timestamp, modified_files, modified_symbols, 
                 modification_type, specialist, success, issues, task_context)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                modification.modification_id,
                modification.timestamp.isoformat(),
                json.dumps(modification.modified_files),
                json.dumps(modification.modified_symbols),
                modification.modification_type,
                modification.specialist,
                modification.success,
                json.dumps(modification.issues),
                modification.task_context
            ))
            conn.commit()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("record_modification", elapsed)
    
    def get_hotspots(self, time_window: Optional[TimeWindow] = None) -> List[Hotspot]:
        """Identify frequently modified areas (hotspots)"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Build query with optional time filter
            if time_window:
                cursor.execute('''
                    SELECT modified_symbols, timestamp 
                    FROM modifications 
                    WHERE timestamp >= ? AND timestamp <= ?
                ''', (time_window.start.isoformat(), time_window.end.isoformat()))
            else:
                # Default to last 30 days
                cutoff = datetime.now() - timedelta(days=30)
                cursor.execute('''
                    SELECT modified_symbols, timestamp 
                    FROM modifications 
                    WHERE timestamp >= ?
                ''', (cutoff.isoformat(),))
            
            rows = cursor.fetchall()
            
            # Count modifications per component
            component_counts = defaultdict(int)
            component_last_modified = {}
            
            for symbols_json, timestamp_str in rows:
                symbols = json.loads(symbols_json)
                for symbol in symbols:
                    component_counts[symbol] += 1
                    timestamp = datetime.fromisoformat(timestamp_str)
                    if symbol not in component_last_modified or timestamp > component_last_modified[symbol]:
                        component_last_modified[symbol] = timestamp
            
            # Create hotspots
            hotspots = []
            for component, count in component_counts.items():
                if count >= 3:  # Only consider components modified 3+ times
                    # Determine trend
                    recent_count = sum(1 for symbols_json, _ in rows 
                                     if component in json.loads(symbols_json))
                    trend = "increasing" if recent_count > count / 2 else "stable"
                    
                    hotspot = Hotspot(
                        component_id=component,
                        modification_frequency=count,
                        time_window=time_window,
                        trend=trend,
                        associated_breakages=self._count_breakages_for_component(component),
                        last_modified=component_last_modified.get(component, datetime.now())
                    )
                    hotspots.append(hotspot)
            
            # Sort by modification frequency
            hotspots.sort(key=lambda h: h.modification_frequency, reverse=True)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("get_hotspots", elapsed)
        
        return hotspots
    
    def _count_breakages_for_component(self, component: str) -> int:
        """Count breakages associated with a component"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) FROM breakages WHERE component_id = ?
            ''', (component,))
            return cursor.fetchone()[0]
    
    def analyze_modification_patterns(self) -> List[ModificationPattern]:
        """Analyze patterns in historical modifications"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT modification_type, specialist, success, modified_files, modified_symbols
                FROM modifications
            ''')
            rows = cursor.fetchall()
            
            # Analyze patterns
            patterns = []
            
            # Group by modification type and specialist
            type_specialist_groups = defaultdict(list)
            for mod_type, specialist, success, files_json, symbols_json in rows:
                key = (mod_type, specialist)
                type_specialist_groups[key].append({
                    'success': success,
                    'files': json.loads(files_json),
                    'symbols': json.loads(symbols_json)
                })
            
            # Generate patterns from groups
            pattern_id = 0
            for (mod_type, specialist), group in type_specialist_groups.items():
                if len(group) >= 2:  # Only patterns seen at least twice
                    success_count = sum(1 for g in group if g['success'])
                    
                    # Find typical components
                    all_symbols = []
                    for g in group:
                        all_symbols.extend(g['symbols'])
                    symbol_counts = defaultdict(int)
                    for symbol in all_symbols:
                        symbol_counts[symbol] += 1
                    typical_components = [s for s, c in symbol_counts.most_common(3)]
                    
                    # Typical outcomes
                    typical_outcomes = {
                        'success': success_count,
                        'failure': len(group) - success_count
                    }
                    
                    pattern = ModificationPattern(
                        pattern_id=f"pattern_{pattern_id}",
                        description=f"{mod_type} by {specialist}",
                        frequency=len(group),
                        typical_components=typical_components,
                        typical_outcomes=typical_outcomes,
                        confidence=0.7 if len(group) > 5 else 0.5,
                        last_seen=datetime.now()  # Could be more precise
                    )
                    patterns.append(pattern)
                    pattern_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_modification_patterns", elapsed)
        
        return patterns


class FragileComponentRegistry:
    """Tracks components that frequently cause issues"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def _initialize_schema(self):
        """Initialize the database schema for fragile component tracking"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS breakages (
                    breakage_id TEXT PRIMARY KEY,
                    component_id TEXT,
                    timestamp DATETIME,
                    breakage_type TEXT,
                    context TEXT,
                    modification_cause TEXT,
                    severity TEXT
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_breakages_component 
                ON breakages(component_id)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_breakages_timestamp 
                ON breakages(timestamp)
            ''')
            conn.commit()
    
    def register_breakage(self, breakage: ComponentBreakage) -> None:
        """Record a component breakage event"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO breakages 
                (breakage_id, component_id, timestamp, breakage_type, 
                 context, modification_cause, severity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                breakage.breakage_id,
                breakage.component_id,
                breakage.timestamp.isoformat(),
                breakage.breakage_type,
                breakage.context,
                breakage.modification_cause,
                breakage.severity.value
            ))
            conn.commit()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("register_breakage", elapsed)
    
    def get_fragile_components(self) -> List[FragileComponent]:
        """Returns components ranked by fragility"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT component_id, breakage_type, timestamp, severity
                FROM breakages
                ORDER BY timestamp DESC
            ''')
            rows = cursor.fetchall()
            
            # Group by component and analyze
            component_data = defaultdict(lambda: {
                'count': 0,
                'types': set(),
                'last_breakage': None
            })
            
            for component_id, breakage_type, timestamp_str, severity in rows:
                component_data[component_id]['count'] += 1
                component_data[component_id]['types'].add(breakage_type)
                timestamp = datetime.fromisoformat(timestamp_str)
                if component_data[component_id]['last_breakage'] is None or timestamp > component_data[component_id]['last_breakage']:
                    component_data[component_id]['last_breakage'] = timestamp
            
            # Create fragile components
            fragile_components = []
            for component_id, data in component_data.items():
                if data['count'] >= 2:  # Only consider components with 2+ breakages
                    # Calculate fragility score (0-1)
                    fragility_score = min(data['count'] / 10.0, 1.0)
                    
                    # Determine risk level
                    if fragility_score > 0.7:
                        risk_level = "critical"
                    elif fragility_score > 0.4:
                        risk_level = "high"
                    else:
                        risk_level = "medium"
                    
                    fragile = FragileComponent(
                        component_id=component_id,
                        fragility_score=fragility_score,
                        breakage_count=data['count'],
                        breakage_types=list(data['types']),
                        last_breakage=data['last_breakage'],
                        risk_level=risk_level
                    )
                    fragile_components.append(fragile)
            
            # Sort by fragility score
            fragile_components.sort(key=lambda f: f.fragility_score, reverse=True)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("get_fragile_components", elapsed)
        
        return fragile_components
    
    def analyze_breakage_patterns(self) -> List[BreakagePattern]:
        """Analyzes patterns in component breakages"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT breakage_type, component_id, context
                FROM breakages
            ''')
            rows = cursor.fetchall()
            
            # Analyze patterns
            patterns = []
            
            # Group by breakage type
            type_groups = defaultdict(list)
            for breakage_type, component_id, context in rows:
                type_groups[breakage_type].append({
                    'component': component_id,
                    'context': context
                })
            
            # Generate patterns
            pattern_id = 0
            for breakage_type, group in type_groups.items():
                if len(group) >= 2:  # Only patterns seen at least twice
                    # Extract component types from component IDs
                    component_types = []
                    for item in group:
                        # Simple heuristic: use first part of component ID as type
                        component_type = item['component'].split('_')[0] if '_' in item['component'] else item['component']
                        component_types.append(component_type)
                    
                    # Common causes from context
                    common_causes = []
                    if group:
                        # Use context as potential cause indicator
                        contexts = [item['context'] for item in group if item['context']]
                        if contexts:
                            common_causes = ["context_related_issue"]  # Could be more sophisticated
                    
                    pattern = BreakagePattern(
                        pattern_id=f"breakage_pattern_{pattern_id}",
                        description=f"{breakage_type} pattern",
                        component_types=list(set(component_types)),
                        common_causes=common_causes,
                        frequency=len(group),
                        confidence=0.6 if len(group) > 5 else 0.4
                    )
                    patterns.append(pattern)
                    pattern_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_breakage_patterns", elapsed)
        
        return patterns


class ArchitecturalDecisionRecorder:
    """Records and retrieves architectural decisions"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def _initialize_schema(self):
        """Initialize the database schema for architectural decision tracking"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS architectural_decisions (
                    decision_id TEXT PRIMARY KEY,
                    title TEXT,
                    context TEXT,
                    decision TEXT,
                    consequences TEXT,
                    components_affected TEXT,
                    timestamp DATETIME,
                    decision_maker TEXT,
                    status TEXT
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_decisions_component 
                ON architectural_decisions(components_affected)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_decisions_timestamp 
                ON architectural_decisions(timestamp)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_decisions_status 
                ON architectural_decisions(status)
            ''')
            conn.commit()
    
    def record_decision(self, decision: ArchitecturalDecision) -> None:
        """Record an architectural decision with context"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO architectural_decisions 
                (decision_id, title, context, decision, consequences, 
                 components_affected, timestamp, decision_maker, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                decision.decision_id,
                decision.title,
                decision.context,
                decision.decision,
                json.dumps(decision.consequences),
                json.dumps(decision.components_affected),
                decision.timestamp.isoformat(),
                decision.decision_maker,
                decision.status
            ))
            conn.commit()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("record_decision", elapsed)
    
    def retrieve_relevant_decisions(self, context: QueryContext) -> List[ArchitecturalDecision]:
        """Retrieves architectural decisions relevant to a context"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Build query based on context
            if context.component_id:
                cursor.execute('''
                    SELECT * FROM architectural_decisions 
                    WHERE components_affected LIKE ?
                    ORDER BY timestamp DESC
                ''', (f'%{context.component_id}%',))
            else:
                cursor.execute('''
                    SELECT * FROM architectural_decisions 
                    ORDER BY timestamp DESC
                ''')
            
            rows = cursor.fetchall()
            
            # Convert to ArchitecturalDecision objects
            decisions = []
            for row in rows:
                decision = ArchitecturalDecision(
                    decision_id=row[0],
                    title=row[1],
                    context=row[2],
                    decision=row[3],
                    consequences=json.loads(row[4]),
                    components_affected=json.loads(row[5]),
                    timestamp=datetime.fromisoformat(row[6]),
                    decision_maker=row[7],
                    status=row[8]
                )
                decisions.append(decision)
            
            # Filter by time window if specified
            if context.time_window:
                decisions = [
                    d for d in decisions 
                    if context.time_window.start <= d.timestamp <= context.time_window.end
                ]
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("retrieve_relevant_decisions", elapsed)
        
        return decisions
    
    def analyze_decision_evolution(self, component: str) -> DecisionEvolution:
        """Analyzes how architectural decisions evolved for a component"""
        start = time.time()
        
        decisions = self.retrieve_relevant_decisions(
            QueryContext(query="", component_id=component)
        )
        
        # Build evolution timeline
        evolution_timeline = [(d.timestamp, d.status) for d in decisions]
        
        # Determine evolution pattern
        if len(decisions) == 0:
            pattern = "no_decisions"
        elif len(decisions) == 1:
            pattern = "single_decision"
        elif all(d.status == "active" for d in decisions):
            pattern = "consistent_active"
        elif any(d.status == "superseded" for d in decisions):
            pattern = "evolving_superseded"
        else:
            pattern = "mixed_status"
        
        evolution = DecisionEvolution(
            component_id=component,
            decisions=decisions,
            evolution_timeline=evolution_timeline,
            current_state=decisions[-1].status if decisions else "unknown",
            evolution_pattern=pattern
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_decision_evolution", elapsed)
        
        return evolution


class KnownRiskRegistry:
    """Tracks known risks and their status"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def _initialize_schema(self):
        """Initialize the database schema for risk tracking"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS known_risks (
                    risk_id TEXT PRIMARY KEY,
                    description TEXT,
                    risk_category TEXT,
                    components_affected TEXT,
                    severity TEXT,
                    likelihood REAL,
                    mitigation_status TEXT,
                    mitigation_strategies TEXT,
                    created_timestamp DATETIME,
                    updated_timestamp DATETIME
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_risks_component 
                ON known_risks(components_affected)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_risks_status 
                ON known_risks(mitigation_status)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_risks_severity 
                ON known_risks(severity)
            ''')
            conn.commit()
    
    def register_risk(self, risk: KnownRisk) -> None:
        """Register a known risk"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO known_risks 
                (risk_id, description, risk_category, components_affected, 
                 severity, likelihood, mitigation_status, mitigation_strategies,
                 created_timestamp, updated_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                risk.risk_id,
                risk.description,
                risk.risk_category,
                json.dumps(risk.components_affected),
                risk.severity.value,
                risk.likelihood,
                risk.mitigation_status,
                json.dumps(risk.mitigation_strategies),
                risk.created_timestamp.isoformat(),
                risk.updated_timestamp.isoformat()
            ))
            conn.commit()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("register_risk", elapsed)
    
    def update_risk_status(self, risk_id: str, status: RiskStatus) -> None:
        """Update the status of a known risk"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE known_risks 
                SET mitigation_status = ?, updated_timestamp = ?
                WHERE risk_id = ?
            ''', (status.value, datetime.now().isoformat(), risk_id))
            conn.commit()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("update_risk_status", elapsed)
    
    def get_active_risks(self, component: Optional[str] = None) -> List[KnownRisk]:
        """Retrieves active risks, optionally filtered by component"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Build query
            if component:
                cursor.execute('''
                    SELECT * FROM known_risks 
                    WHERE mitigation_status != ? AND components_affected LIKE ?
                    ORDER BY severity DESC, updated_timestamp DESC
                ''', ('closed', f'%{component}%'))
            else:
                cursor.execute('''
                    SELECT * FROM known_risks 
                    WHERE mitigation_status != ?
                    ORDER BY severity DESC, updated_timestamp DESC
                ''', ('closed',))
            
            rows = cursor.fetchall()
            
            # Convert to KnownRisk objects
            risks = []
            for row in rows:
                risk = KnownRisk(
                    risk_id=row[0],
                    description=row[1],
                    risk_category=row[2],
                    components_affected=json.loads(row[3]),
                    severity=row[4],  # This is the string value
                    likelihood=row[5],
                    mitigation_status=row[6],
                    mitigation_strategies=json.loads(row[7]),
                    created_timestamp=datetime.fromisoformat(row[8]),
                    updated_timestamp=datetime.fromisoformat(row[9])
                )
                risks.append(risk)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("get_active_risks", elapsed)
        
        return risks
