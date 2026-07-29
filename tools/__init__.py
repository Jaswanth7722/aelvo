# __init__.py - Central Tool Registry Builder for AELVO OMEGA

from typing import Dict, Any
from tools.code_tools import run_linter, run_formatter, run_type_checker, run_tests, build_symbol_graph
from tools.security_tools import scan_for_secrets, scan_for_vulnerabilities, simulate_attack
from tools.diagram_tools import generate_mermaid_flowchart, generate_mermaid_mindmap, validate_mermaid
from tools.git_tools import get_git_state, generate_commit_message, detect_merge_conflicts, generate_pr_description
from tools.research_tools import decompose_query, rank_source_credibility, build_wiki_entry

def build_extended_tool_registry(fs, kernel, memory_engine) -> Dict[str, Dict[str, Any]]:
    """Constructs the unified system-wide tool registry mapping tool names to execution wrappers."""
    # Define wrappers to sanitize arguments sent by the LLM
    workspace_root = str(fs.base_path)

    def _wrap_linter(path, language=None, **_ignored):
        return run_linter(path, language or "python", workspace_root)

    def _wrap_formatter(path, language=None, **_ignored):
        return run_formatter(path, language or "python", workspace_root)

    def _wrap_type_checker(path, language=None, **_ignored):
        return run_type_checker(path, language or "python", workspace_root)

    def _wrap_tests(target, language=None, test_filter="", **_ignored):
        return run_tests(target, language or "python", test_filter or "", workspace_root)

    def _wrap_symbol_graph(**_ignored):
        return build_symbol_graph(workspace_root)
        
    def _wrap_secrets(path, **_ignored):
        return scan_for_secrets(path, workspace_root)
        
    def _wrap_vulns(path, **_ignored):
        return scan_for_vulnerabilities(path, workspace_root)
        
    def _wrap_simulate_attack(vulnerability_type, path, vulnerable_code, **_ignored):
        return simulate_attack(vulnerability_type, path, vulnerable_code)
        
    def _wrap_flowchart(components, connections, **_ignored):
        return generate_mermaid_flowchart(components, connections)
        
    def _wrap_mindmap(structure, **_ignored):
        return generate_mermaid_mindmap(structure)
        
    def _wrap_validate_mermaid(diagram, **_ignored):
        ok, msg = validate_mermaid(diagram)
        return {"status": "success" if ok else "error", "logs": msg, "executed": {}}
        
    def _wrap_git_state(**_ignored):
        return get_git_state(workspace_root)
        
    def _wrap_commit_message(**_ignored):
        return generate_commit_message(workspace_root)
        
    def _wrap_merge_conflicts(**_ignored):
        return detect_merge_conflicts(workspace_root)
        
    def _wrap_pr_description(base_branch, head_branch, **_ignored):
        return generate_pr_description(base_branch, head_branch, workspace_root)
        
    def _wrap_decompose_query(query, **_ignored):
        return decompose_query(query)
        
    def _wrap_rank_source(url, **_ignored):
        return rank_source_credibility(url)
        
    def _wrap_build_wiki(topic, synthesis_data, contradictions=None, **_ignored):
        return build_wiki_entry(topic, synthesis_data, contradictions)

    # Return structured dict
    return {
        "run_linter": {
            "fn": _wrap_linter,
            "required_constraints": [],
            "constraints_map": {}
        },
        "run_formatter": {
            "fn": _wrap_formatter,
            "required_constraints": [],
            "constraints_map": {}
        },
        "run_type_checker": {
            "fn": _wrap_type_checker,
            "required_constraints": [],
            "constraints_map": {}
        },
        "run_tests": {
            "fn": _wrap_tests,
            "required_constraints": [],
            "constraints_map": {}
        },
        "build_symbol_graph": {
            "fn": _wrap_symbol_graph,
            "required_constraints": [],
            "constraints_map": {}
        },
        "scan_for_secrets": {
            "fn": _wrap_secrets,
            "required_constraints": [],
            "constraints_map": {}
        },
        "scan_for_vulnerabilities": {
            "fn": _wrap_vulns,
            "required_constraints": [],
            "constraints_map": {}
        },
        "simulate_attack": {
            "fn": _wrap_simulate_attack,
            "required_constraints": [],
            "constraints_map": {}
        },
        "generate_mermaid_flowchart": {
            "fn": _wrap_flowchart,
            "required_constraints": [],
            "constraints_map": {}
        },
        "generate_mermaid_mindmap": {
            "fn": _wrap_mindmap,
            "required_constraints": [],
            "constraints_map": {}
        },
        "validate_mermaid": {
            "fn": _wrap_validate_mermaid,
            "required_constraints": [],
            "constraints_map": {}
        },
        "get_git_state": {
            "fn": _wrap_git_state,
            "required_constraints": [],
            "constraints_map": {}
        },
        "generate_commit_message": {
            "fn": _wrap_commit_message,
            "required_constraints": [],
            "constraints_map": {}
        },
        "detect_merge_conflicts": {
            "fn": _wrap_merge_conflicts,
            "required_constraints": [],
            "constraints_map": {}
        },
        "generate_pr_description": {
            "fn": _wrap_pr_description,
            "required_constraints": [],
            "constraints_map": {}
        },
        "decompose_query": {
            "fn": _wrap_decompose_query,
            "required_constraints": [],
            "constraints_map": {}
        },
        "rank_source_credibility": {
            "fn": _wrap_rank_source,
            "required_constraints": [],
            "constraints_map": {}
        },
        "build_wiki_entry": {
            "fn": _wrap_build_wiki,
            "required_constraints": [],
            "constraints_map": {}
        }
    }
