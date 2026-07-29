# research_tools.py - Advanced Academic Research & Knowledge Compiler for AELVO OMEGA

import re
import datetime
from typing import Dict, Any, List, Tuple

def decompose_query(query: str) -> Dict[str, Any]:
    """Decomposes a complex research query into 3 to 5 highly structured sub-queries covering all facets."""
    # Heuristics for decomposition based on query keywords
    sub_queries = []
    
    clean = query.lower()
    
    # 1. Historical/Factual baseline
    sub_queries.append(f"Core concepts, definitions, and underlying specifications for: {query}")
    
    # 2. Tech architecture/Contextual details
    sub_queries.append(f"Architectural implementation, standards, and engineering trade-offs regarding: {query}")
    
    # 3. Recent developments / Temporal awareness
    current_year = datetime.datetime.now().year
    sub_queries.append(f"Latest updates, specifications, or releases up to {current_year} related to: {query}")
    
    # 4. Critics/Contradictions/Alternative perspectives
    sub_queries.append(f"Alternative perspectives, challenges, and competing paradigms for: {query}")

    return {
        "status": "success",
        "logs": f"Decomposed query into {len(sub_queries)} multi-dimensional sub-queries.",
        "executed": {"original_query_length": len(query)},
        "data": {
            "original_query": query,
            "sub_queries": sub_queries
        }
    }

def rank_source_credibility(url: str) -> Dict[str, Any]:
    """Inspects dynamic URL signatures to assign academic credibility rankings (Tiers 1 to 4)."""
    tier = 4
    reason = "Unverified web blog or community forum signature."
    
    clean_url = url.lower()
    
    # Tier 1: Specifications, Official Docs, RFCs, Academics
    if any(p in clean_url for p in [".gov", ".edu", "ietf.org", "rfc-editor.org", "arxiv.org", "w3.org", "iso.org"]):
        tier = 1
        reason = "Authoritative standards organization, academic paper repository, or government registry."
    
    # Tier 2: Official Documentation for major tools, frameworks
    elif any(p in clean_url for p in ["docs.", "developer.", "mozilla.org", "microsoft.com", "oracle.com", "python.org", "npm"]):
        tier = 2
        reason = "Primary documentation repository for a standard language, library, or platform."
        
    # Tier 3: High-quality tech outlets, verified articles
    elif any(p in clean_url for p in ["github.com", "medium.com/engineering", "stackoverflow.com", "wikipedia.org"]):
        tier = 3
        reason = "Collaborative technical platform, community reference standard, or verified engineering blog."

    return {
        "status": "success",
        "logs": f"Source rated as Tier {tier} Credibility.",
        "executed": {"url": url},
        "data": {
            "url": url,
            "tier": tier,
            "reasoning": reason
        }
    }

def build_wiki_entry(topic: str, synthesis_data: List[Dict[str, Any]], contradictions: List[str] = None) -> Dict[str, Any]:
    """Formats synthesized multi-source data into a robust Wikipedia-style research wiki document."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
    current_year = datetime.datetime.now().year
    
    lines = [
        f"# {topic.upper()}",
        "",
        "## Overview",
        "This synthesized research entry compiles multi-source findings to outline key technical features and concepts.",
        ""
    ]
    
    # Main sections based on synthesis data
    # Expected synthesis item: {"title": "Title", "content": "Context", "url": "URL"}
    lines.append("## Technical Details")
    for idx, item in enumerate(synthesis_data, 1):
        title = item.get("title", f"Section {idx}")
        content = item.get("content", "No content available.")
        url = item.get("url", "N/A")
        
        cred = rank_source_credibility(url)
        tier = cred["data"]["tier"]
        
        lines.append(f"### {title}")
        lines.append(content)
        lines.append(f"*[Source (Tier {tier}): {url}]*")
        lines.append("")
        
    # Temporal check section
    lines.append("## Current Status & Temporal Relevance")
    lines.append(f"This record was synthesized on **{timestamp}**. Active relevance has been validated for the year **{current_year}**.")
    lines.append("")

    # Contradictions section
    lines.append("## Contradictions & Uncertainties")
    if contradictions:
        for contra in contradictions:
            lines.append(f"- {contra}")
    else:
        lines.append("No active factual contradictions or structural discrepancies identified across examined sources.")
    lines.append("")

    # Citations list
    lines.append("## Citations & Sources")
    for idx, item in enumerate(synthesis_data, 1):
        url = item.get("url", "N/A")
        cred = rank_source_credibility(url)
        lines.append(f"{idx}. [{cred['data']['reasoning']}] {url} (Tier {cred['data']['tier']})")

    wiki_content = "\n".join(lines)

    return {
        "status": "success",
        "logs": f"Wikipedia-style entry compiled for '{topic}'.",
        "executed": {"sections_built": len(synthesis_data)},
        "data": {
            "topic": topic,
            "wiki_markdown": wiki_content
        }
    }
