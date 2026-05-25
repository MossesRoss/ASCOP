import subprocess
import time
import os

def run_demo():
    services = [
        "src/ingestion.py",
        "src/agents.py",
        "src/guardrail.py",
        "src/execution.py"
    ]
    processes = []
    venv_python = "venv/bin/python3"

    print("--- ASCOP LIVE DEMO STARTING ---")
    print("Simulating Autonomous Supply Chain Orchestration for 30 seconds...")
    
    for service in services:
        p = subprocess.Popen([venv_python, service])
        processes.append(p)
        print(f"Launched {service}")

    try:
        # Let it run for 30 seconds
        time.sleep(30)
    except KeyboardInterrupt:
        pass
    finally:
        print("\n--- ASCOP LIVE DEMO TEARDOWN ---")
        for p in processes:
            p.terminate()
        print("All services stopped.")

if __name__ == "__main__":
    run_demo()
