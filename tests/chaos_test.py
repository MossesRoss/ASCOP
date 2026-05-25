import subprocess
import time
import json
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"

def run_chaos_test():
    venv_python = "venv/bin/python3"
    
    # 1. Start the Remediation Agent
    surgeon = subprocess.Popen([venv_python, "src/remediation.py"])
    time.sleep(2)

    # 2. Inject a critical infrastructure fault
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)
    
    fault_event = {
        "event_type": "Infrastructure_Fault_Event",
        "fault_type": "API_TIMEOUT",
        "component": "Supplier_Gateway_v1",
        "severity": "CRITICAL",
        "timestamp": time.time()
    }
    
    print(f"\n[CHAOS] Injecting fault: {fault_event['fault_type']}...")
    producer.produce("infrastructure_faults", json.dumps(fault_event).encode('utf-8'))
    producer.flush()

    # 3. Wait to see the remediation in action
    time.sleep(6)
    
    surgeon.terminate()
    print("\n[CHAOS] Test complete.")

if __name__ == "__main__":
    run_chaos_test()
