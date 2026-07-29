# diagram_tools.py - Mermaid Diagramming & Structural Synthesis for AELVO OMEGA

import re
from typing import Dict, Any, List, Tuple

def validate_mermaid(diagram: str) -> Tuple[bool, str]:
    """Validates Mermaid syntax to avoid diagram rendering crashes (e.g. checking bracket alignment)."""
    lines = diagram.strip().splitlines()
    if not lines:
        return False, "Diagram is empty."

    # Validate opening syntax
    header = lines[0].strip().lower()
    valid_headers = ["graph", "flowchart", "sequenceDiagram", "classDiagram", "stateDiagram", "erDiagram", "gantt", "pie", "gitGraph", "mindmap"]
    if not any(header.startswith(h) for h in valid_headers):
        return False, f"Invalid Mermaid header. Must start with one of: {', '.join(valid_headers)}"

    # Check bracket matching (e.g. (), [], {}, [""], etc.)
    brackets = {
        '(': ')',
        '[': ']',
        '{': '}'
    }
    
    stack = []
    in_quotes = False
    
    for idx, line in enumerate(lines, 1):
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
                continue
            
            if in_quotes:
                continue
                
            if char in brackets:
                stack.append((char, idx))
            elif char in brackets.values():
                if not stack:
                    return False, f"Unbalanced bracket '{char}' found on line {idx}."
                top_char, top_line = stack.pop()
                if brackets[top_char] != char:
                    return False, f"Mismatched bracket '{top_char}' on line {top_line} with '{char}' on line {idx}."

    if stack:
        top_char, top_line = stack.pop()
        return False, f"Unclosed bracket '{top_char}' opened on line {top_line}."

    return True, "Mermaid syntax validated successfully."

def generate_mermaid_flowchart(components: List[Dict[str, str]], connections: List[Dict[str, str]]) -> Dict[str, Any]:
    """Generates a valid, verified Mermaid flowchart of components and data streams."""
    lines = ["flowchart TD"]
    
    # Render component definitions
    # Expected component: {"id": "A", "label": "API", "shape": "round"}
    # Shapes: round -> (), box -> [], cylinder -> [( )], database -> [( )]
    for comp in components:
        c_id = comp.get("id")
        label = comp.get("label", c_id)
        shape = comp.get("shape", "box")
        
        # Guard against special character rendering breaks by wrapping labels in double quotes
        safe_label = f'"{label}"'
        
        if shape == "round":
            lines.append(f"    {c_id}({safe_label})")
        elif shape in ("cylinder", "database"):
            lines.append(f"    {c_id}[({safe_label})]")
        elif shape == "stadium":
            lines.append(f"    {c_id}([{safe_label}])")
        elif shape == "subroutine":
            lines.append(f"    {c_id}[[{safe_label}]]")
        else:
            lines.append(f"    {c_id}[{safe_label}]")

    # Render connections
    # Expected connection: {"from": "A", "to": "B", "label": "HTTPS"}
    for conn in connections:
        c_from = conn.get("from")
        c_to = conn.get("to")
        label = conn.get("label", "")
        
        if label:
            lines.append(f"    {c_from} -->|{label}| {c_to}")
        else:
            lines.append(f"    {c_from} --> {c_to}")

    diagram = "\n".join(lines)
    ok, err_msg = validate_mermaid(diagram)
    
    return {
        "status": "success" if ok else "error",
        "logs": err_msg if not ok else "Flowchart generated and verified successfully.",
        "executed": {"nodes": len(components), "connections": len(connections)},
        "data": {"diagram": diagram}
    }

def generate_mermaid_mindmap(structure: Dict[str, Any]) -> Dict[str, Any]:
    """Walks the mapped structure dictionary and outputs a valid Mermaid mindmap graph."""
    # Expected structure: {"root": "AELVO", "children": [{"name": "Kernel", "children": [...]}]}
    lines = ["mindmap", "    root((AELVO OMEGA))"]
    
    def walk_node(node: Dict[str, Any], indent_level: int = 2):
        name = node.get("name", "Node")
        children = node.get("children", [])
        
        indent = "    " * indent_level
        # Sanitize name of any characters that might break mermaid rendering
        safe_name = name.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
        
        if children:
            lines.append(f"{indent}{safe_name}")
            for child in children:
                walk_node(child, indent_level + 1)
        else:
            lines.append(f"{indent}{safe_name}")

    if "children" in structure:
        for child in structure["children"]:
            walk_node(child, 2)
            
    diagram = "\n".join(lines)
    ok, err_msg = validate_mermaid(diagram)
    
    return {
        "status": "success" if ok else "error",
        "logs": err_msg if not ok else "Mindmap generated and verified successfully.",
        "executed": {"nodes_walked": len(lines) - 2},
        "data": {"diagram": diagram}
    }
