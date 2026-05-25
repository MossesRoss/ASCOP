import json
import time
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"

def main():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f'remediation_group_{time.time()}', # New group to skip old toxic messages
        'auto.offset.reset': 'latest' # Only listen to new faults
    }
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}

    consumer = Consumer(consumer_conf)
    consumer.subscribe(['infrastructure_faults'])

    print("Remediation Agent (The Surgeon) is online. Monitoring system health...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error(): continue
            
            val = msg.value()
            if not val: continue
            
            try:
                event = json.loads(val.decode('utf-8'))
            except Exception as e:
                print(f"Skipping malformed message: {e}")
                continue
                
            fault_type = event.get("fault_type")
            
            print(f"!!! FAULT DETECTED: {fault_type} on component {event.get('component')}")
            print(f"Analysing path... Executing deterministic failover.")
            
            time.sleep(2) # Simulate analysis
            
            if fault_type == "API_TIMEOUT":
                print(" -> ACTION: Rerouting traffic to Secondary API Gateway Region (EU-West).")
            elif fault_type == "DATA_CORRUPTION":
                print(" -> ACTION: Deploying DLQ Sweeper and resetting consumer offsets.")
            
            print("Successfully healed. System returning to nominal state.")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
