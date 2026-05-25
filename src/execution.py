import json
import psycopg2
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "ascop_ledger",
    "user": "ascop_admin",
    "password": "ascop_password"
}

def main():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'execution_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(['approved_actions'])

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("Execution & ERP Synchronization Plane started.")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode('utf-8'))
            event_type = event['event_type']
            item_id = event['item_id']

            print(f"Executing Approved Action: {event_type} for {item_id}")

            # 1. Log to Immutable Ledger
            cur.execute(
                "INSERT INTO events_ledger (event_type, payload) VALUES (%s, %s)",
                (event_type, json.dumps(event))
            )

            # 2. Sync with ERP
            if event_type == "Proposed_Purchase_Order_Event":
                quantity = event['quantity']
                cur.execute(
                    "UPDATE erp_inventory SET quantity = quantity + %s, last_updated = CURRENT_TIMESTAMP WHERE item_id = %s",
                    (quantity, item_id)
                )
            elif event_type == "Proposed_Price_Change_Event":
                # This logic is simplified; in reality, we'd calculate the new price
                # Here we just log the change intent in the ledger
                pass

            conn.commit()
            print(f"Successfully synchronized {event_type} with ERP.")

    except KeyboardInterrupt:
        pass
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()
