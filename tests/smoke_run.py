import os
import asyncio
import tempfile
import traceback

from core.execution import AelvoKernel
from core.filesystem import AelvoFileSystem
from core.governance import MemoryEngine
from core.orchestration import Orchestrator
from core.provider_runtime import init_provider_runtime

async def main():
    print("Starting smoke boot...")
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "memory.db")
            anchor_path = os.path.join(temp_dir, "anchor.md")
            workspace_path = os.path.join(temp_dir, "workspace")
            vault_path = os.path.join(temp_dir, "auth_credentials.db")
            os.makedirs(workspace_path, exist_ok=True)
            
            test_file = os.path.join(workspace_path, "test.txt")
            with open(test_file, "w", encoding="utf-8") as f:
                f.write("Hello AELVO")

            with open(anchor_path, "w", encoding="utf-8") as f:
                f.write("---\nmeta: Test Anchor\nversion: 1.0\n---\n# Test\n")

            print("Init kernel...")
            kernel = AelvoKernel(db_path=db_path, anchor_path=anchor_path)
            
            print("Init FS...")
            fs = AelvoFileSystem(base_path=workspace_path, kernel=kernel)
            
            print("Init memory engine...")
            memory_engine = MemoryEngine(db_path=db_path, anchor_path=anchor_path, tool_registry={}, project_name="smoke_test")
            
            print("Init provider runtime...")
            provider_runtime = await init_provider_runtime(vault_path=vault_path, runtime_dir=temp_dir)
            
            print("Init orchestrator...")
            orchestrator = Orchestrator(memory_engine=memory_engine, kernel=kernel, base_path=workspace_path, provider_runtime=provider_runtime)
            
            print("Executing minimal tool call...")
            result = fs.read_file("test.txt")
            assert result["status"] == "success"
            assert "Hello AELVO" in result["data"]
            
            print("Activating FORGE...", flush=True)
            class MockAgent:
                conversation_history = []
                
            agent = MockAgent()
            turn_result = await orchestrator.execute_turn(agent, "@FORGE what Python version is this project using")
            
            print(f"Turn Result: {turn_result}", flush=True)
            print("Success!", flush=True)

            # Cleanup
            memory_engine.db.close()
            kernel.conn.close()
            if hasattr(orchestrator, "runtime_bus") and hasattr(orchestrator.runtime_bus, "stop"):
                await getattr(orchestrator.runtime_bus, "stop")()
            
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
