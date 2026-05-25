import json
import time
import redis
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
REDIS_HOST = "localhost"
REDIS_PORT = 6380

def diagnose(message):
    """Simulates LLM Diagnosis logic"""
    if "SQL Timeout" in message or "Deadlock" in message:
        return {
            "diagnosis": "Missing index on `erp_inventory` table causing full table scans during concurrent updates.",
            "proposed_fix": "CREATE INDEX idx_item_id_inventory ON erp_inventory(item_id);",
            "impact": "High. Will reduce query latency from 2s to <50ms."
        }
    elif "Memory overflow" in message:
        return {
            "diagnosis": "Heap exhaustion in batch worker due to large single-transaction commit size.",
            "proposed_fix": "ALTER SYSTEM SET batch_commit_size = 1000;",
            "impact": "Medium. Prevents system crashes during peak load."
        }
    return {
        "diagnosis": "Unknown anomaly detected in SAP module.",
        "proposed_fix": "Enable verbose logging for 1 hour to gather more data.",
        "impact": "Low."
    }

def main():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'diagnostic_agent_group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['erp_logs'])
    
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    print("Diagnostic Agent (LLM-Simulator) started. Monitoring ERP logs...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error(): continue

            log_event = json.loads(msg.value().decode('utf-8'))
            raw_msg = log_event.get("message")
            
            print(f"Analyzing log: {raw_msg}")
            
            # Simulate LLM Thinking time
            time.sleep(2)
            
            analysis = diagnose(raw_msg)
            
            proposal = {
                "event_type": "Proposed_Fix_Event",
                "raw_log": raw_msg,
                "diagnosis": analysis['diagnosis'],
                "proposed_fix": analysis['proposed_fix'],
                "impact": analysis['impact'],
                "timestamp": time.time()
            }
            
            # Route directly to HITL Queue
            r.lpush("hitl_queue", json.dumps(proposal))
            print(f"Proposal pushed to HITL: {proposal['proposed_fix']}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
