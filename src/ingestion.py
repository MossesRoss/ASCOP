import json
import random
import time
import asyncio
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

async def produce_market_events(producer):
    items = ["widget_a", "widget_b"]
    while True:
        item = random.choice(items)
        price_delta = random.uniform(-5.0, 5.0)
        event = {
            "event_type": "Market_Price_Event",
            "item_id": item,
            "price_delta": price_delta,
            "timestamp": time.time()
        }
        producer.produce("market_events", json.dumps(event).encode('utf-8'), callback=delivery_report)
        producer.flush()
        await asyncio.sleep(5)

async def produce_inventory_events(producer):
    items = ["widget_a", "widget_b"]
    while True:
        item = random.choice(items)
        if random.random() < 0.2:
            event = {
                "event_type": "Inventory_Deficit_Event",
                "item_id": item,
                "deficit_quantity": random.randint(10, 50),
                "timestamp": time.time()
            }
            producer.produce("inventory_events", json.dumps(event).encode('utf-8'), callback=delivery_report)
            producer.flush()
        await asyncio.sleep(10)

async def produce_erp_errors(producer):
    # Simulating raw SAP application logs
    errors = [
        "SQL Timeout at line 42: Missing index on erp_inventory table causing full table scan during batch update.",
        "Memory overflow in module FI_AR_001 during batch processing of 10,000 records.",
        "Deadlock detected in module SD_04_01. Waiting for lock on table erp_inventory."
    ]
    while True:
        await asyncio.sleep(15)  # Inject an error every 15 seconds for the demo
        raw_error = random.choice(errors)
        event = {
            "event_type": "Raw_ERP_Log_Event",
            "source": "SAP_ECC_6",
            "raw_log": raw_error,
            "timestamp": time.time()
        }
        producer.produce("erp_logs", json.dumps(event).encode('utf-8'), callback=delivery_report)
        producer.flush()
        print(f"Produced ERP Error Log: {event['raw_log']}")

async def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)
    
    print("Starting Ingestion Plane (including SAP error logs)...")
    await asyncio.gather(
        produce_market_events(producer),
        produce_inventory_events(producer),
        produce_erp_errors(producer) # The new ERP error stream
    )

if __name__ == "__main__":
    asyncio.run(main())