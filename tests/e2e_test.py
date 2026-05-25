import subprocess
import time
import json
import psycopg2
import redis
from confluent_kafka import Producer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
REDIS_PORT = 6380
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "ascop_ledger",
    "user": "ascop_admin",
    "password": "ascop_password"
}

def start_services():
    services = ["src/agents.py", "src/guardrail.py", "src/execution.py"]
    processes = []
    venv_python = "venv/bin/python3"
    for service in services:
        p = subprocess.Popen([venv_python, service])
        processes.append(p)
        print(f"Started {service} (PID: {p.pid})")
    return processes

def inject_test_events():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)

    # 1. Safe Event: Purchase Order under budget
    safe_purchase = {
        "event_type": "Inventory_Deficit_Event",
        "item_id": "widget_a",
        "deficit_quantity": 10, # 10 * 100 = 1000 (Safe)
        "timestamp": time.time()
    }
    
    # 2. Vetoed Event: Purchase Order OVER budget
    risky_purchase = {
        "event_type": "Inventory_Deficit_Event",
        "item_id": "widget_b",
        "deficit_quantity": 50, # 50 * 500 = 25000 (Vetoed)
        "timestamp": time.time()
    }

    producer.produce("inventory_events", json.dumps(safe_purchase).encode('utf-8'))
    producer.produce("inventory_events", json.dumps(risky_purchase).encode('utf-8'))
    producer.flush()
    print("Injected test events.")

def verify_results():
    print("Verifying results...")
    time.sleep(10) # Wait for processing

    # Check Postgres Ledger
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT event_type, payload FROM events_ledger")
    ledger_entries = cur.fetchall()
    print(f"Ledger Entries ({len(ledger_entries)}):")
    for entry in ledger_entries:
        print(f" - {entry[0]}")

    # Check Redis HITL Queue
    r = redis.Redis(host="localhost", port=REDIS_PORT, decode_responses=True)
    hitl_count = r.llen("hitl_queue")
    print(f"HITL Queue size: {hitl_count}")
    if hitl_count > 0:
        last_veto = json.loads(r.lpop("hitl_queue"))
        print(f"Last Vetoed Action: {last_veto['event_type']} (Amount: {last_veto.get('amount')})")

    cur.close()
    conn.close()

    # Assertions
    assert any(e[0] == "Proposed_Purchase_Order_Event" for e in ledger_entries), "Safe event should be in ledger"
    assert hitl_count > 0, "Risky event should be in HITL queue"
    print("\n[SUCCESS] E2E Test Passed!")

if __name__ == "__main__":
    procs = start_services()
    try:
        inject_test_events()
        verify_results()
    finally:
        for p in procs:
            p.terminate()
        print("Services stopped.")
