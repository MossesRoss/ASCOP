import json
import time
import redis
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
REDIS_HOST = "localhost"
REDIS_PORT = 6380

def get_diagnosis_and_fix(raw_log):
    # Simulating an LLM diagnosing the root cause based on log context
    if "Cyclone" in raw_log or "NOAA" in raw_log:
        return (
            "Anticipated 40% disruption in Guntur Red Chilli raw supply chain and immediate 25% price spike post-cyclone.",
            "Autonomous PO #8892: Procure 50 MT of Guntur Red Chillies from secondary inland suppliers in Karnataka at current market price.",
            "Secures raw material inventory for 3 months, avoiding estimated ₹15,00,000 price premium."
        )
    elif "SQL Timeout" in raw_log or "Missing index" in raw_log:
        return (
            "Missing index on erp_inventory table causing full table scan during concurrent updates.",
            "CREATE INDEX idx_item_id_inventory ON erp_inventory(item_id);",
            "High. Will reduce query latency from 2s to <10ms."
        )
    elif "Memory overflow" in raw_log:
        return (
            "Heap exhaustion in batch worker due to large single-transaction commit size.",
            "ALTER SYSTEM SET batch_commit_size = 1000;",
            "Medium. Prevents system crashes during peak load."
        )
    elif "Deadlock" in raw_log:
        return (
            "Transaction collision between procurement agent and execution plane on erp_inventory.",
            "Implement ROW EXCLUSIVE locks during quantity updates.",
            "Critical. Prevents data corruption and failed PO processing."
        )
    return ("Unknown error.", "Requires manual DBA investigation.", "Unknown")

def main():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'diagnostic_agent_group',
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(['erp_logs'])
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    print("Diagnostic Agent is online. Listening for ERP error logs and OSINT alerts...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error(): continue
            
            event = json.loads(msg.value().decode('utf-8'))
            if event.get('event_type') in ['Raw_ERP_Log_Event', 'Raw_OSINT_Alert']:
                raw_log = event['raw_log']
                print(f"Analyzing System Log: {raw_log}")
                
                # Fake LLM processing time
                time.sleep(2) 
                
                diagnosis, proposed_fix, impact = get_diagnosis_and_fix(raw_log)
                
                fix_proposal_event = {
                    "event_type": "Proposed_Fix_Event",
                    "source_system": event.get("source"),
                    "raw_error": raw_log,
                    "diagnosis": diagnosis,
                    "proposed_fix": proposed_fix,
                    "expected_impact": impact,
                    "timestamp": time.time()
                }
                
                print("Diagnosis complete. Routing proposed action to HITL queue.")
                r.lpush("hitl_queue", json.dumps(fix_proposal_event))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()