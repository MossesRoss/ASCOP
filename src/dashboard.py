import streamlit as st
import pandas as pd
import psycopg2
import redis
import json
import time

# Configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "ascop_ledger",
    "user": "ascop_admin",
    "password": "ascop_password"
}
REDIS_HOST = "localhost"
REDIS_PORT = 6380

st.set_page_config(page_title="ASCOP Command Center", layout="wide")

# Initialize connections
def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def get_redis_conn():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

st.title("ASCOP: Autonomous Supply Chain Orchestration Protocol")
st.write("Real-time Enterprise Self-Healing & Guardrail Interface")

# Tabs
tab1, tab2, tab3 = st.tabs(["📊 ERP Operations", "🛡️ Economic Guardrail (HITL)", "📜 Audit Ledger"])

with tab1:
    st.header("Live ERP State (Simulated)")
    try:
        conn = get_db_conn()
        query = "SELECT * FROM erp_inventory"
        df = pd.read_sql(query, conn)
        st.table(df)
        conn.close()
    except Exception as e:
        st.error(f"Error connecting to Ledger: {e}")

with tab2:
    st.header("HITL Queue: Pending High-Priority Remediations")
    r = get_redis_conn()
    try:
        queue_items = r.lrange("hitl_queue", 0, -1)
    except Exception as e:
        st.error(f"Error connecting to Redis: {e}")
        queue_items = []
    
    if not queue_items:
        st.info("No pending actions in the HITL queue.")
    else:
        for idx, item in enumerate(queue_items):
            data = json.loads(item)
            
            # Different rendering for Proposed_Fix_Event
            if data.get("event_type") == "Proposed_Fix_Event":
                with st.expander(f"🔴 HIGH PRIORITY: AI Diagnosis - {data.get('proposed_fix')[:30]}...", expanded=True):
                    st.error(f"**Raw SAP Error:** `{data.get('raw_log')}`")
                    st.info(f"**AI Diagnosis:** {data.get('diagnosis')}")
                    st.warning(f"**Proposed Fix:** `{data.get('proposed_fix')}`")
                    st.write(f"**Expected Impact:** {data.get('impact')}")
                    
                    col1, col2 = st.columns(2)
                    if col1.button("Approve & Execute", key=f"approve_{idx}"):
                        # In a real system, this would push back to an 'approved_fixes' topic
                        st.success("Executing remediation script...")
                        r.lrem("hitl_queue", 1, item)
                        st.rerun()
                    if col2.button("Reject", key=f"reject_{idx}"):
                        r.lrem("hitl_queue", 1, item)
                        st.rerun()
            else:
                with st.expander(f"Action: {data.get('event_type')}"):
                    st.write(data)
                    if st.button("Dismiss", key=f"dismiss_{idx}"):
                        r.lrem("hitl_queue", 1, item)
                        st.rerun()

with tab3:
    st.header("Immutable Event Ledger")
    try:
        conn = get_db_conn()
        query = "SELECT * FROM events_ledger ORDER BY created_at DESC LIMIT 20"
        df = pd.read_sql(query, conn)
        st.dataframe(df, use_container_width=True)
        conn.close()
    except Exception as e:
        st.error(f"Error connecting to Ledger: {e}")

# Auto-refresh
time.sleep(2)
st.rerun()
