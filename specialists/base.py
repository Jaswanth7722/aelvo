# base.py - Core Contract Interface for AELVO OMEGA Specialists

import asyncio
from typing import List, Dict, Tuple, Any, Optional

class BaseSpecialist:
    """Standardized abstract base contract representing an AELVO specialist node."""
    name: str                          # Specialist identity
    trigger_patterns: List[str]        # Keywords that activate this specialist
    memory_types: List[str]            # Memory types this specialist owns
    required_tools: List[str]          # Tools this specialist needs registered
    activation_threshold: float = 0.6  # Confidence threshold for activation

    def get_system_prompt(self, context: Dict[str, Any]) -> str:
        """Build dynamic prompt from live memory + filesystem + constraints.
        
        Never return a static string.
        """
        raise NotImplementedError("Specialists must implement get_system_prompt()")

    def post_process(self, result: str, memory_engine, conversation_history: List[Dict[str, Any]]) -> str:
        """Extract signal from what just happened, save to memory, return audit summary."""
        raise NotImplementedError("Specialists must implement post_process()")

    def compute_activation_score(self, task: str, context: Dict[str, Any]) -> float:
        """Return 0.0–1.0 confidence that this specialist should handle this task.
        
        Use keyword matching + memory query + task structure analysis.
        """
        score = 0.0
        clean_task = task.lower()
        
        # 1. Keyword matching
        matches = sum(1 for pattern in self.trigger_patterns if pattern.lower() in clean_task)
        if matches > 0:
            score += min(0.4, matches * 0.15)
            
        # 2. Memory query (simplified for score computation)
        memory_engine = context.get("memory_engine")
        if memory_engine and self.memory_types:
            try:
                # Use the memory engine's search capability to check for relevance
                results = memory_engine.search(task, limit=1, types=self.memory_types)
                if results and results[0].get("score", 0) > 0.7:
                    score += 0.3
            except Exception as _ex: print("Silenced exception: %s", _ex)
                
        return min(1.0, score)

    def build_memory_context(self, task: str, memory_engine) -> Dict[str, Any]:
        """Query this specialist's memory collection for relevant context.
        
        Return structured dict for injection into system prompt.
        """
        raise NotImplementedError("Specialists must implement build_memory_context()")

    def verify_output(self, output: str, context: Dict[str, Any]) -> Tuple[bool, str]:
        """Verify the specialist's output before allow respond.
        
        Return (verified: bool, reason: str).
        """
        raise NotImplementedError("Specialists must implement verify_output()")

    async def execute(self, task: str, context: Dict[str, Any]) -> str:
        """Execute this specialist's role on the given task.

        Default implementation:
        1. Builds the system prompt via get_system_prompt()
        2. Combines it with the user task
        3. Sends through the LLM agent

        Specialists may override this method to provide custom execution
        logic (e.g., HERMES creates HermesContext, ARCHITECT creates
        ArchitectPlan). The context dict MUST contain an 'agent' key
        for the default implementation to work.

        Args:
            task: The user task / request string.
            context: Shared context dict (must include 'agent' for LLM dispatch).

        Returns:
            The specialist's output as a string.
        """
        agent = context.get("agent")
        if agent is None:
            raise ValueError(
                f"{self.name}.execute() requires 'agent' in context. "
                f"Pass the TurnAgent instance via context['agent']."
            )

        # Build the specialist's system prompt with full context
        system_prompt = self.get_system_prompt(context)

        # Combine system prompt with the user task
        combined_prompt = (
            f"{system_prompt}\n\n"
            f"{'=' * 60}\n"
            f"USER TASK:\n"
            f"{'=' * 60}\n\n"
            f"{task}\n\n"
            f"Respond with your specialist output based on the instructions above."
        )

        # Send through the LLM agent (synchronous call in async context)
        raw_output = agent.send_user_message(combined_prompt)
        if isinstance(raw_output, str):
            return raw_output
        return str(raw_output)

    # ── Provider Runtime Integration ──────────────────────────────

    def get_provider_runtime_prompt(self, context: Dict[str, Any]) -> str:
        """Build a prompt injection for provider runtime if available in context.

        Specialists can call this in their get_system_prompt() to include
        provider discovery, health checks, and model capability information
        in their system prompt.
        """
        runtime = context.get("provider_runtime")
        if not runtime:
            return ""

        lines = [
            "PROVIDER RUNTIME AVAILABLE:",
            f"  {len(runtime.provider_configs)} providers registered",
            f"  {len(runtime.model_registry.list_models())} models available",
        ]

        providers_with_creds = [
            pid for pid in runtime.provider_configs
            if runtime.has_credentials(pid)
        ]
        if providers_with_creds:
            lines.append(f"  Providers with credentials: {', '.join(sorted(providers_with_creds))}")

        active = runtime.get_active_providers()
        if active:
            lines.append(f"  Active: {', '.join(sorted(active))}")

        # Add capability summary for providers with credentials
        lines.append("")
        lines.append("Use #providers list, #providers health, or #providers models [provider]")
        lines.append("to inspect the provider runtime from the command line.")

        return "\n".join(lines)

    def get_provider_capabilities_prompt(
        self, context: Dict[str, Any], provider_key: str
    ) -> str:
        """Get a prompt injection for a specific provider's capabilities."""
        runtime = context.get("provider_runtime")
        if not runtime:
            return ""

        config = runtime.get_provider_config(provider_key)
        if not config:
            return f"Provider '{provider_key}' not found in registry."

        health_status = (
            runtime.health.get_status(provider_key).name
            if runtime.registry.has_provider(provider_key)
            else "UNKNOWN"
        )
        has_creds = runtime.has_credentials(provider_key)

        models = config.models
        capabilities_list = sorted(c.name for c in config.capabilities)

        lines = [
            f"PROVIDER: {config.name} ({provider_key})",
            f"  Status: {health_status}",
            f"  Credentials: {'✅ Configured' if has_creds else '❌ Not configured'}",
            f"  SDK: {config.sdk_type or 'unknown'}",
            f"  Base URL: {config.base_url or 'default'}",
            f"  Models ({len(models)}): {', '.join(m.id for m in models[:5])}",
            f"  Capabilities ({len(capabilities_list)}): {', '.join(capabilities_list)}",
            f"  Default Model: {config.default_model}",
        ]

        return "\n".join(lines)
