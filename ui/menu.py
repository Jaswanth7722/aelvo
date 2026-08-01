# menu.py - Sleek Dark-Mode CLI Menus & Boot Logos for AELVO OMEGA

import os
import sys
import sqlite3
from config.settings import BASE_DIR
from ui.style import (
    print_styled, draw_header, draw_separator, C_PRIMARY, C_ACCENT, C_SUCCESS, C_WARNING, C_DANGER, C_MUTED, C_WHITE, SYM_OK, SYM_INFO, SYM_WARN, SYM_FAIL, SYM_BULLET
)
import logging

log = logging.getLogger(__name__)


# Paths resolved via BASE_DIR
GLOBAL_DB_PATH = os.path.join(BASE_DIR, "global_memory.db")
GLOBAL_ANCHOR_PATH = os.path.join(BASE_DIR, "global_anchor.md")
WORKSPACE_BASE = os.path.join(BASE_DIR, "workspace")

def init_global_metadata():
    """Ensures the global database for tracking projects is ready."""
    try:
        db = sqlite3.connect(GLOBAL_DB_PATH)
        db.execute("""
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                description TEXT,
                path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_opened TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS user_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        # Scaffold global anchor if missing
        if not os.path.exists(GLOBAL_ANCHOR_PATH):
            with open(GLOBAL_ANCHOR_PATH, "w", encoding="utf-8") as f:
                f.write("---\nmeta: AELVO Global Constraints\nversion: 1.0\n---\n# Global Rules\nAll projects inherit these root constraints.\n")
        db.commit()
        db.close()
    except Exception as e:
        print_styled(f"{SYM_FAIL} Global Init Error: {e}", C_DANGER)

def select_project_interactive() -> str:
    """Interactive boot menu for project selection with stunning styling."""
    init_global_metadata()
    
    # Header block
    draw_header("AELVO OMEGA", "Autonomous Multi-Agent Workspace Director")
    
    db = sqlite3.connect(GLOBAL_DB_PATH)
    projects = db.execute("SELECT name, description, last_opened FROM projects ORDER BY last_opened DESC").fetchall()
    
    print()
    if projects:
        print_styled(f" {SYM_BULLET} ACTIVE WORKSPACE ARCHIVES:", C_PRIMARY, bold=True)
        draw_separator()
        for i, (name, desc, last) in enumerate(projects):
            # Shorten last opened string for better alignment
            short_time = str(last)[:16] if last else "N/A"
            idx_str = f"[{i+1}]".ljust(5)
            name_str = name.ljust(18)
            time_str = f"|  {short_time}  |".ljust(23)
            desc_str = desc or "No description provided."
            print_styled(f"   {C_ACCENT}{idx_str}{C_WHITE}{name_str}{C_MUTED}{time_str}{C_SUCCESS}{desc_str}")
        draw_separator()
        print()
        print_styled(f" {SYM_BULLET} AVAILABLE DIRECTIVES:", C_PRIMARY, bold=True)
        print_styled(f"   {C_ACCENT}[N]{C_WHITE}  Establish a New Autonomous Project Workspace")
        print_styled(f"   {C_ACCENT}[D]{C_WHITE}  Decommission & Purge Workspace Files")
    else:
        print_styled(f"   {SYM_WARN} No project workspaces detected.", C_WARNING)
        draw_separator()
        print()
        print_styled(f" {SYM_BULLET} AVAILABLE DIRECTIVES:", C_PRIMARY, bold=True)
        print_styled(f"   {C_ACCENT}[N]{C_WHITE}  Establish Your First Autonomous Workspace")
        
    print_styled(f"   {C_ACCENT}[X]{C_WHITE}  Close and Exit Console Dashboard")
    draw_separator()
    print()
    
    choice = input("Select a directive options: ").strip().upper()
    
    if choice == "N":
        print()
        print_styled("╔══════════════════════════════════════════════════════════════════════╗", C_PRIMARY)
        print_styled("║                  ESTABLISH NEW AUTONOMOUS WORKSPACE                 ║", C_WHITE, bold=True)
        print_styled("╚══════════════════════════════════════════════════════════════════════╝", C_PRIMARY)
        name = input("  Enter workspace name: ").strip()
        if not name: return select_project_interactive()
        desc = input("  Enter project description: ").strip()
        try:
            db.execute("INSERT INTO projects (name, description, path) VALUES (?, ?, ?)", 
                       (name, desc, os.path.join(WORKSPACE_BASE, name)))
            db.commit()
            target_name = name
            print_styled(f"\n{SYM_OK} Established new workspace: '{name}'!", C_SUCCESS)
        except sqlite3.IntegrityError:
            print_styled(f"\n{SYM_FAIL} Workspace '{name}' already exists in registries.", C_DANGER)
            db.close()
            return select_project_interactive()
    elif choice == "D" and projects:
        print()
        del_choice = input("Enter project index number to delete: ").strip()
        if del_choice.isdigit() and 1 <= int(del_choice) <= len(projects):
            del_name = projects[int(del_choice)-1][0]
            print_styled(f"\n{SYM_WARN} WARNING: You are about to permanently delete '{del_name}' and all associated workspace files.", C_WARNING, bold=True)
            confirm = input("Type 'yes' to verify destruction: ").strip().lower()
            if confirm == "yes":
                import shutil
                # Remove from database registry
                db.execute("DELETE FROM projects WHERE name = ?", (del_name,))
                db.commit()
                # Purge from physical disk
                try:
                    shutil.rmtree(os.path.join(WORKSPACE_BASE, del_name), ignore_errors=True)
                    print_styled(f"{SYM_OK} Completed destruction. Registry wiped.", C_SUCCESS)
                except Exception as e:
                    print_styled(f"{SYM_FAIL} Error removing files: {e}", C_DANGER)
            else:
                print_styled("Aborted.", C_MUTED)
        db.close()
        return select_project_interactive()
    elif choice.isdigit() and 1 <= int(choice) <= len(projects):
        target_name = projects[int(choice)-1][0]
        db.execute("UPDATE projects SET last_opened = CURRENT_TIMESTAMP WHERE name = ?", (target_name,))
        db.commit()
    elif choice == "X":
        print_styled("\nExiting. Systems offline.\n", C_MUTED)
        sys.exit(0)
    else:
        db.close()
        return select_project_interactive()
    
    db.close()
    return target_name

def interactive_provider_setup(model_registry: dict) -> tuple:
    """Styled wizard setup for configuring API credentials."""
    draw_header("AELVO GATEWAY CONFIGURATION", "Select and authorize your primary model provider")
    print()
    
    providers = list(model_registry.keys())
    for i, name in enumerate(providers):
        print_styled(f"   {C_ACCENT}[{i+1}]{C_WHITE}  {name.upper()}")
    print_styled(f"   {C_ACCENT}[0]{C_WHITE}  Abort and Exit Setup Wizard")
    draw_separator()
    print()
    
    choice = input("Select a provider option: ").strip()
    if choice.isdigit() and 1 <= int(choice) <= len(providers):
        p_name = providers[int(choice)-1]
        cfg = model_registry[p_name]
        
        print()
        print_styled(f"  Configuring {p_name.upper()}...", C_PRIMARY, bold=True)
        api_key = input(f"  Enter API Key for {p_name.upper()} ({cfg.env_key}): ").strip()
        model_name = input(f"  Enter Model Name (leave blank for '{cfg.default_model}'): ").strip()
        
        env_path = os.path.join(BASE_DIR, ".env")
        try:
            lines = []
            if os.path.exists(env_path):
                with open(env_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            
            # Remove old keys out of .env to rewrite them cleanly
            lines = [l for l in lines if not l.startswith(cfg.env_key + "=") and not l.startswith("LLM_PROVIDER=") and not l.startswith("LLM_MODEL=")]
            
            # Save API key to encrypted credential store instead of .env
            from auth.cred_storage import CredentialStore
            from auth.types import Credential, CredentialType
            import uuid
            import time

            db_path = os.path.join(BASE_DIR, ".aelvo_runtime", "credential_vault.db")
            store = CredentialStore(db_path=db_path)
            
            cred = Credential(
                id=f"key_{p_name}_{uuid.uuid4().hex[:8]}",
                provider=p_name,
                credential_type=CredentialType.API_KEY,
                value=api_key,
                label=f"{p_name} API key (Configured via Wizard)",
                created_at=time.time(),
                is_valid=True,
                metadata={"source": "wizard_setup"},
            )
            store.store(cred)
            print_styled("\n[SECURITY] Saved API key securely to encrypted vault.", C_SUCCESS)

            # Comment out in .env to prevent plaintext storage
            lines.append(f"# {cfg.env_key} (Migrated to encrypted credential store vault)\n")
            lines.append(f"LLM_PROVIDER={p_name}\n")
            if model_name:
                lines.append(f"LLM_MODEL={model_name}\n")
            
            with open(env_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            print_styled(f"\n{SYM_OK} Configuration saved successfully (keys encrypted in vault)!", C_SUCCESS)
            os.environ[cfg.env_key] = api_key
            os.environ["LLM_PROVIDER"] = p_name
            if model_name:
                os.environ["LLM_MODEL"] = model_name
            elif "LLM_MODEL" in os.environ:
                del os.environ["LLM_MODEL"]
                
            return p_name, cfg, api_key, model_name or cfg.default_model
        except Exception as e:
            print_styled(f"{SYM_FAIL} Failed to save credentials: {e}", C_DANGER)
            sys.exit(1)
    else:
        print_styled("\nSetup aborted.", C_MUTED)
        sys.exit(0)

def migrate_env_keys():
    """Migrates API keys from .env to the encrypted credential store vault, then deletes them from .env."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        migrated = False
        new_lines = []
        
        from auth.cred_storage import CredentialStore
        from auth.types import Credential, CredentialType
        import uuid
        import time
        
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".aelvo_runtime", "credential_vault.db")
        store = CredentialStore(db_path=db_path)
        
        # We also need model registry to know the key names
        from core.registry import MODEL_REGISTRY
        env_to_provider = {cfg.env_key: name for name, cfg in MODEL_REGISTRY.items()}
        
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                new_lines.append(line)
                continue
            
            if "=" in stripped:
                parts = stripped.split("=", 1)
                key = parts[0].strip()
                val = parts[1].strip().strip('"').strip("'")
                
                # Check if this matches one of the provider API keys
                if key in env_to_provider and val and val not in ("your-api-key-here", "your-anthropic-api-key-here", ""):
                    provider_name = env_to_provider[key]
                    # Check if already exists in store
                    existing = store.get_for_provider(provider_name)
                    if not existing or existing.value != val:
                        # Store in vault
                        cred = Credential(
                            id=f"key_{provider_name}_{uuid.uuid4().hex[:8]}",
                            provider=provider_name,
                            credential_type=CredentialType.API_KEY,
                            value=val,
                            label=f"{provider_name} API key (Migrated from .env)",
                            created_at=time.time(),
                            is_valid=True,
                            metadata={"source": "env_migration"},
                        )
                        store.store(cred)
                        print(f"[SECURITY] Migrated API key '{key}' to encrypted vault.")
                    
                    # Comment out in .env to prevent plaintext storage
                    new_lines.append(f"# {key} (Migrated to encrypted credential store vault)\n")
                    migrated = True
                    continue
            
            new_lines.append(line)
            
        if migrated:
            with open(env_path, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            print("[SECURITY] Plaintext API keys removed from .env file successfully.")
            
    except Exception as e:
        print(f"[SECURITY] API key migration failed: {e}")


def detect_provider(model_registry: dict) -> tuple:
    """Auto-detects active credentials or launches wizard setup."""
    # First, run migration of plaintext .env keys to the encrypted store
    migrate_env_keys()

    if '--config' in sys.argv:
        res = interactive_provider_setup(model_registry)
        if res: return res

    explicit = os.environ.get("LLM_PROVIDER", "").strip().lower()
    model_override = os.environ.get("LLM_MODEL", "").strip()

    if explicit:
        if explicit not in model_registry:
            print_styled(f"{SYM_WARN} Unknown LLM_PROVIDER='{explicit}' in environment variables.", C_WARNING)
            return interactive_provider_setup(model_registry)
        cfg = model_registry[explicit]
        key = os.environ.get(cfg.env_key, "")
        if not key:
            try:
                from auth.cred_storage import CredentialStore
                db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".aelvo_runtime", "credential_vault.db")
                store = CredentialStore(db_path=db_path)
                cred = store.get_for_provider(explicit)
                if cred:
                    key = cred.value
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)
        if not key:
            print_styled(f"{SYM_WARN} LLM_PROVIDER is '{explicit}' but {cfg.env_key} variable is missing.", C_WARNING)
            return interactive_provider_setup(model_registry)
        model = model_override or cfg.default_model
        return explicit, cfg, key, model

    # Auto-detect: scan for the first available API key in environment
    for name, cfg in model_registry.items():
        key = os.environ.get(cfg.env_key, "")
        if key and key not in ("your-api-key-here", "your-anthropic-api-key-here", ""):
            model = model_override or cfg.default_model
            return name, cfg, key, model

    # Try to scan for keys in CredentialStore
    try:
        from auth.cred_storage import CredentialStore
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".aelvo_runtime", "credential_vault.db")
        store = CredentialStore(db_path=db_path)
        for name, cfg in model_registry.items():
            cred = store.get_for_provider(name)
            if cred and cred.value:
                model = model_override or cfg.default_model
                return name, cfg, cred.value, model
    except Exception as _ex:
        log.warning("Silenced exception: %s", _ex)

    # Nothing found -> Interactive Wizard
    print_styled(f"\n{SYM_INFO} No active LLM provider keys detected in environmental vars, .env or credential store.", C_PRIMARY)
    return interactive_provider_setup(model_registry)

def show_boot_logo(provider_name: str, model: str, sdk_type: str, db_path: str, anchor_path: str, workspace_path: str):
    """Draws a premium state-of-the-art terminal start-up logo with vital stats."""
    draw_header("A E L V O   O M E G A   O N L I N E", "Hardened Deterministic Multi-Specialist Core")
    
    # Info blocks
    print_styled(f"   {C_ACCENT}⚡ Provider  :{C_WHITE}  {provider_name.upper()} ({model})")
    print_styled(f"   {C_ACCENT}🔒 SDK Type  :{C_WHITE}  {sdk_type.upper()} SDK (Secure Adapter)")
    print_styled(f"   {C_ACCENT}📂 Workspace :{C_WHITE}  {workspace_path}")
    print_styled(f"   {C_ACCENT}🗄️ Database  :{C_WHITE}  {os.path.basename(db_path)} (SQLite + ChromaDB Hybrid Sync)")
    print_styled(f"   {C_ACCENT}🎯 Anchor    :{C_WHITE}  {os.path.basename(anchor_path)} (Active Hard Constraints)")
    
    draw_separator()
    print_styled(f"   {SYM_INFO} Commands  :  Type naturally to prompt specialists or use #commands.", C_PRIMARY)
    print_styled(f"   {SYM_WARN} Exit      :  Type 'exit', 'quit', or press Ctrl+C to terminate.", C_WARNING)
    draw_separator()
    print()
