import subprocess
import time

def run_aha_demo():
    services = [
        "src/ingestion.py",
        "src/agents.py",
        "src/guardrail.py",
        "src/execution.py",
        "src/remediation.py",
        "src/diagnostic_agent.py"
    ]
    processes = []
    venv_python = "venv/bin/python3"

    print("--- ASCOP 'AHA!' DEMO STARTING ---")
    print("Launching all planes and the new Diagnostic Agent...")
    
    for service in services:
        p = subprocess.Popen([venv_python, service])
        processes.append(p)
        print(f"Launched {service}")

    print("\nDashboard can be started with: venv/bin/streamlit run src/dashboard.py")
    print("Simulating for 60 seconds to allow SAP errors and AI diagnosis to appear...")

    try:
        time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        print("\n--- ASCOP DEMO TEARDOWN ---")
        for p in processes:
            p.terminate()
        print("All background services stopped.")

if __name__ == "__main__":
    run_aha_demo()
