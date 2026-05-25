import json
import httpx
import redis
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
OPA_URL = "http://localhost:8182/v1/data/ascop/guardrail/allow"
REDIS_HOST = "localhost"
REDIS_PORT = 6380

def main():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'guardrail_group',
        'auto.offset.reset': 'earliest'
    }
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    consumer.subscribe(['proposed_actions'])

    print("Economic Guardrail Layer started. Protecting the system...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode('utf-8'))
            print(f"Evaluating event: {event['event_type']} for {event.get('item_id')}")

            # Query OPA
            try:
                response = httpx.post(OPA_URL, json={"input": event})
                result = response.json().get("result", False)
            except Exception as e:
                print(f"OPA query failed: {e}")
                result = False # Default to deny on error

            if result:
                print(f" [PASS] Action approved: {event['event_type']}")
                producer.produce("approved_actions", json.dumps(event).encode('utf-8'))
                producer.flush()
            else:
                print(f" [VETO] Action rejected! Routing to HITL queue: {event['event_type']}")
                r.lpush("hitl_queue", json.dumps(event))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
