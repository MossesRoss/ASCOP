import os
import json
import time
import requests
import redis
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
REDIS_HOST = "localhost"
REDIS_PORT = 6380

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable is missing. It is required for the Diagnostic Agent.")

def get_diagnosis_and_fix(raw_log):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    payload = {
        "contents": [{
            "parts": [{"text": f"Diagnose the root cause and propose a fix for the following log event: {raw_log}"}]
        }],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "object",
                "properties": {
                    "diagnosis": {"type": "string"},
                    "proposed_fix": {"type": "string"},
                    "expected_impact": {"type": "string"}
                },
                "required": ["diagnosis", "proposed_fix", "expected_impact"]
            }
        }
    }
    
    try:
        response = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        content_text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "{}")
        result = json.loads(content_text)
        
        return (
            result.get("diagnosis", "Unknown diagnosis."),
            result.get("proposed_fix", "Unknown fix."),
            result.get("expected_impact", "Unknown impact.")
        )
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        return ("Unknown error. LLM API failed.", "Requires manual DBA investigation.", "Unknown impact.")

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