import json
import time
from confluent_kafka import Consumer, Producer, KafkaException

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"

def main():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'agents_group',
        'auto.offset.reset': 'earliest'
    }
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)

    consumer.subscribe(['market_events', 'inventory_events'])

    print("Multi-Agent Engine started. Listening for events...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()

            if topic == 'market_events':
                # Pricing Agent Logic
                item_id = event['item_id']
                price_delta = event['price_delta']
                
                # Propose a price change based on delta (simplified MARL)
                proposal = {
                    "event_type": "Proposed_Price_Change_Event",
                    "item_id": item_id,
                    "change_percent": abs(price_delta),
                    "direction": "increase" if price_delta > 0 else "decrease",
                    "timestamp": time.time()
                }
                print(f"Pricing Agent proposing: {proposal}")
                producer.produce("proposed_actions", json.dumps(proposal).encode('utf-8'))
                producer.flush()

            elif topic == 'inventory_events':
                # Procurement Agent Logic
                item_id = event['item_id']
                deficit = event['deficit_quantity']
                
                # Propose a purchase order
                # Assume unit price for budget calculation
                unit_price = 100 if item_id == 'widget_a' else 500
                total_amount = deficit * unit_price

                proposal = {
                    "event_type": "Proposed_Purchase_Order_Event",
                    "item_id": item_id,
                    "quantity": deficit,
                    "amount": total_amount,
                    "timestamp": time.time()
                }
                print(f"Procurement Agent proposing: {proposal}")
                producer.produce("proposed_actions", json.dumps(proposal).encode('utf-8'))
                producer.flush()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
