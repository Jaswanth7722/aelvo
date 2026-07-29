"""
test_adversarial.py — Adversarial Testing Suite

Required Scenarios:
- Hallucinated research
- Malicious code suggestion
- Provider outage
- Tool failure
- Security vulnerability
- Bad implementation
- Corrupted evidence
- Conflicting findings
- Consensus disagreement
- Execution failure

Measures: Detection, Containment, Recovery, Escalation, Correction
"""

import time
from typing import Any, Callable, Dict


class AdversarialTestRunner:
    """Runs adversarial scenarios and measures system response."""

    def __init__(self):
        self.scenarios: Dict[str, Dict[str, Any]] = {}

    SCENARIO_DESCRIPTIONS = {
        "hallucinated_research": "ORACLE returns research results with fabricated sources",
        "malicious_code_suggestion": "FORGE suggests code containing a backdoor",
        "provider_outage": "LLM provider returns 503 errors during execution",
        "tool_failure": "A critical tool crashes or returns garbage output",
        "security_vulnerability": "SENTINEL must detect intentional vulnerability in code",
        "bad_implementation": "FORGE produces implementation that doesn't match specification",
        "corrupted_evidence": "Blackboard entry data is corrupted or malicious",
        "conflicting_findings": "ORACLE and another source produce contradictory findings",
        "consensus_disagreement": "Specialists cannot reach consensus on a critical decision",
        "execution_failure": "TERMINUS command fails with non-zero exit code",
    }

    def run_scenario(
        self,
        scenario_name: str,
        system_fn: Callable,
        description: str = "",
    ) -> Dict[str, Any]:
        """Run an adversarial scenario against the system.

        Args:
            scenario_name: One of the 10 required scenario names.
            system_fn: Function that executes the system under test.
            description: Human-readable description.

        Returns:
            Dict with detection, containment, recovery, escalation, correction flags.
        """
        start = time.time()
        try:
            result = system_fn(scenario_name)
            if result is None:
                result = {}
        except Exception as e:
            result = {"error": str(e)}

        duration_ms = (time.time() - start) * 1000

        outcome = {
            "scenario": scenario_name,
            "description": description or self.SCENARIO_DESCRIPTIONS.get(scenario_name, ""),
            "detected": result.get("detected", False),
            "contained": result.get("contained", False),
            "recovered": result.get("recovered", False),
            "escalated": result.get("escalated", False),
            "corrected": result.get("corrected", False),
            "duration_ms": round(duration_ms, 2),
        }
        self.scenarios[scenario_name] = outcome
        return outcome

    def generate_report(self) -> str:
        """Generate a markdown adversarial test report."""
        lines = [
            "# ADVERSARIAL_REPORT.md",
            "",
            "## Adversarial Testing Results",
            "",
            f"Total scenarios: {len(self.scenarios)}",
            "",
        ]
        for name, result in self.scenarios.items():
            lines.append(f"### {result.get('description', name)}")
            lines.append("")
            lines.append("| Metric | Result |")
            lines.append("|--------|--------|")
            lines.append(f"| Detected | {'✓' if result['detected'] else '✗'} |")
            lines.append(f"| Contained | {'✓' if result['contained'] else '✗'} |")
            lines.append(f"| Recovered | {'✓' if result['recovered'] else '✗'} |")
            lines.append(f"| Escalated | {'✓' if result['escalated'] else '✗'} |")
            lines.append(f"| Corrected | {'✓' if result['corrected'] else '✗'} |")
            lines.append(f"| Duration | {result['duration_ms']:.1f}ms |")
            lines.append("")

        # Summary
        total = len(self.scenarios)
        detected = sum(1 for r in self.scenarios.values() if r["detected"])
        contained = sum(1 for r in self.scenarios.values() if r["contained"])
        recovered = sum(1 for r in self.scenarios.values() if r["recovered"])

        lines.append("### Summary")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Total Scenarios | {total} |")
        lines.append(f"| Detection Rate | {detected}/{total} ({detected/total:.0%})" if total > 0 else "| Detection Rate | N/A |")
        lines.append(f"| Containment Rate | {contained}/{total} ({contained/total:.0%})" if total > 0 else "| Containment Rate | N/A |")
        lines.append(f"| Recovery Rate | {recovered}/{total} ({recovered/total:.0%})" if total > 0 else "| Recovery Rate | N/A |")
        lines.append("")

        return "\n".join(lines)


# === Test stubs ===

class TestAdversarialFramework:
    """Verify the adversarial testing framework is correctly structured."""

    def test_runner_initializes(self):
        runner = AdversarialTestRunner()
        assert runner is not None
        assert hasattr(runner, "run_scenario")
        assert hasattr(runner, "generate_report")
        assert runner.SCENARIO_DESCRIPTIONS is not None
        assert len(runner.SCENARIO_DESCRIPTIONS) == 10

    def test_all_scenarios_defined(self):
        required = [
            "hallucinated_research",
            "malicious_code_suggestion",
            "provider_outage",
            "tool_failure",
            "security_vulnerability",
            "bad_implementation",
            "corrupted_evidence",
            "conflicting_findings",
            "consensus_disagreement",
            "execution_failure",
        ]
        runner = AdversarialTestRunner()
        for scenario in required:
            assert scenario in runner.SCENARIO_DESCRIPTIONS, f"Missing scenario: {scenario}"

    def test_scenario_execution(self):
        runner = AdversarialTestRunner()

        def mock_system(scenario):
            return {
                "detected": True,
                "contained": True,
                "recovered": True,
                "escalated": False,
                "corrected": True,
            }

        for scenario_name in runner.SCENARIO_DESCRIPTIONS:
            result = runner.run_scenario(scenario_name, mock_system)
            assert result["scenario"] == scenario_name
            assert "detected" in result
            assert "contained" in result

    def test_report_generation(self):
        runner = AdversarialTestRunner()

        def mock_system(scenario):
            return {"detected": True, "contained": True, "recovered": True,
                    "escalated": False, "corrected": True}

        for scenario_name in runner.SCENARIO_DESCRIPTIONS:
            runner.run_scenario(scenario_name, mock_system)

        report = runner.generate_report()
        assert "ADVERSARIAL_REPORT.md" in report
        assert "Total scenarios: 10" in report
        assert "Detection Rate" in report
        # Verify key report sections exist
        assert "detected" in report.lower() or "Detection Rate" in report
        assert "Total scenarios" in report
        assert "10" in report
