"""
AELVO Event Factory
====================
Factory functions for creating typed events with proper structure.
"""

import uuid
from typing import Any, Dict, Optional
from .event_bus import Event, EventType


def create_task_event(
    event_type: EventType,
    task_id: str,
    task_name: str,
    specialist: str,
    status: str,
    progress: float = 0.0,
    metadata: Optional[Dict[str, Any]] = None
) -> Event:
    """
    Create a task-related event.
    
    Args:
        event_type: The specific task event type
        task_id: Unique task identifier
        task_name: Human-readable task name
        specialist: Specialist responsible for the task
        status: Current task status
        progress: Task progress (0.0 to 1.0)
        metadata: Additional task metadata
        
    Returns:
        Configured task event
    """
    data = {
        "task_id": task_id,
        "task_name": task_name,
        "specialist": specialist,
        "status": status,
        "progress": progress
    }
    
    if metadata:
        data.update(metadata)
    
    return Event(
        event_type=event_type,
        data=data,
        source="task_manager",
        correlation_id=task_id
    )


def create_specialist_event(
    event_type: EventType,
    specialist: str,
    action: str,
    details: Optional[Dict[str, Any]] = None
) -> Event:
    """
    Create a specialist-related event.
    
    Args:
        event_type: The specific specialist event type
        specialist: Specialist name
        action: Action being performed
        details: Additional details
        
    Returns:
        Configured specialist event
    """
    data = {
        "specialist": specialist,
        "action": action
    }
    
    if details:
        data.update(details)
    
    return Event(
        event_type=event_type,
        data=data,
        source="specialist_manager",
        correlation_id=str(uuid.uuid4())
    )


def create_tool_event(
    event_type: EventType,
    tool_name: str,
    command: str,
    status: str = "running",
    output: Optional[str] = None,
    exit_code: Optional[int] = None,
    duration: Optional[float] = None
) -> Event:
    """
    Create a tool-related event.
    
    Args:
        event_type: The specific tool event type
        tool_name: Name of the tool
        command: Command being executed
        status: Tool status
        output: Tool output (if available)
        exit_code: Process exit code (if available)
        duration: Execution duration in seconds (if available)
        
    Returns:
        Configured tool event
    """
    data = {
        "tool_name": tool_name,
        "command": command,
        "status": status
    }
    
    if output:
        data["output"] = output
    if exit_code is not None:
        data["exit_code"] = exit_code
    if duration is not None:
        data["duration"] = duration
    
    return Event(
        event_type=event_type,
        data=data,
        source="tool_executor",
        correlation_id=str(uuid.uuid4())
    )


def create_memory_event(
    event_type: EventType,
    memory_type: str,
    query: Optional[str] = None,
    result_count: int = 0,
    relevance_score: float = 0.0,
    memories: Optional[list] = None
) -> Event:
    """
    Create a memory-related event.
    
    Args:
        event_type: The specific memory event type
        memory_type: Type of memory (episodic, semantic, procedural)
        query: Query that triggered memory retrieval
        result_count: Number of memories retrieved
        relevance_score: Average relevance score
        memories: Retrieved memories (if available)
        
    Returns:
        Configured memory event
    """
    data = {
        "memory_type": memory_type,
        "result_count": result_count,
        "relevance_score": relevance_score
    }
    
    if query:
        data["query"] = query
    if memories:
        data["memories"] = memories
    
    return Event(
        event_type=event_type,
        data=data,
        source="memory_engine",
        correlation_id=str(uuid.uuid4())
    )


def create_verification_event(
    event_type: EventType,
    verification_type: str,
    target: str,
    status: str,
    details: Optional[str] = None,
    confidence: float = 0.0
) -> Event:
    """
    Create a verification-related event.
    
    Args:
        event_type: The specific verification event type
        verification_type: Type of verification (lint, typecheck, test, security)
        target: Target being verified
        status: Verification status
        details: Additional details
        confidence: Confidence in the result
        
    Returns:
        Configured verification event
    """
    data = {
        "verification_type": verification_type,
        "target": target,
        "status": status,
        "confidence": confidence
    }
    
    if details:
        data["details"] = details
    
    return Event(
        event_type=event_type,
        data=data,
        source="verification_engine",
        correlation_id=str(uuid.uuid4())
    )


def create_safety_event(
    event_type: EventType,
    action: str,
    risk_level: str,
    reason: str,
    requires_approval: bool = False,
    impact_assessment: Optional[str] = None
) -> Event:
    """
    Create a safety-related event.
    
    Args:
        event_type: The specific safety event type
        action: Action being evaluated
        risk_level: Risk level (low, medium, high, critical)
        reason: Reason for safety check
        requires_approval: Whether approval is required
        impact_assessment: Assessment of potential impact
        
    Returns:
        Configured safety event
    """
    data = {
        "action": action,
        "risk_level": risk_level,
        "reason": reason,
        "requires_approval": requires_approval
    }
    
    if impact_assessment:
        data["impact_assessment"] = impact_assessment
    
    return Event(
        event_type=event_type,
        data=data,
        source="safety_governance",
        correlation_id=str(uuid.uuid4())
    )


def create_system_event(
    event_type: EventType,
    message: str,
    details: Optional[Dict[str, Any]] = None
) -> Event:
    """
    Create a system-related event.
    
    Args:
        event_type: The specific system event type
        message: System message
        details: Additional details
        
    Returns:
        Configured system event
    """
    data = {
        "message": message
    }
    
    if details:
        data.update(details)
    
    return Event(
        event_type=event_type,
        data=data,
        source="system",
        correlation_id=str(uuid.uuid4())
    )


def create_collaboration_event(
    event_type: EventType,
    specialist: str,
    action: str,
    details: Optional[Dict[str, Any]] = None
) -> Event:
    """
    Create a collaboration-related event (findings, challenges, decisions).

    Args:
        event_type: The specific collaboration event type
        specialist: Specialist name involved
        action: Short description of the collaboration action
        details: Additional collaboration context (entry_id, challenge_id, confidence, etc.)

    Returns:
        Configured collaboration event
    """
    data = {
        "specialist": specialist,
        "action": action,
    }
    if details:
        data.update(details)

    return Event(
        event_type=event_type,
        data=data,
        source="collaboration_engine",
        correlation_id=str(uuid.uuid4()),
    )