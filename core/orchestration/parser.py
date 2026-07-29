# core/orchestration/parser.py - LLM Output Parser for AELVO OMEGA
import json
import logging

log = logging.getLogger("aelvo.parser")

def parse_llm_output(text: str):
    """
    Parses LLM output into a typed payload.
    Supports single dict or a list of dicts (Batched Execution).
    Returns: (output_type, payload)
    types: "kernel_command", "tool_calls", "unknown"
    """
    text = text.strip()

    # 1. Check for # Kernel Command
    if text.startswith("#"):
        return ("kernel_command", [text])

    # 2. Check for JSON Tool Call(s) — Array or Object
    def normalize_calls(data):
        if isinstance(data, list): 
            return [x for x in data if isinstance(x, dict) and "tool" in x]
        if isinstance(data, dict) and "tool" in data:
            return [data]
        return None

    # First, try to find a code block
    try:
        if "```json" in text:
            block = text.split("```json")[1].split("```")[0].strip()
            parsed = json.loads(block, strict=False)
            norm = normalize_calls(parsed)
            if norm: return ("tool_calls", norm)
    except Exception as _ex:
        log.debug("Parser: code block JSON decode failed: %s", _ex)

    # Try direct parse
    try:
        parsed = json.loads(text, strict=False)
        norm = normalize_calls(parsed)
        if norm: return ("tool_calls", norm)
    except Exception as _ex:
        log.debug("Parser: direct JSON decode failed: %s", _ex)

    # Aggressive Search via JSONDecoder
    decoder = json.JSONDecoder(strict=False)
    for marker in ['[', '{']:
        start = text.find(marker)
        if start != -1:
            try:
                candidate = text[start:]
                parsed, index = decoder.raw_decode(candidate)
                norm = normalize_calls(parsed)
                if norm: return ("tool_calls", norm)
            except Exception:
                continue

    return ("unknown", text)
