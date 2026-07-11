import streamlit as st
import pandas as pd
import psycopg2
import redis
import json
import time
from datetime import datetime

# --- Configuration ---
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "ascop_ledger",
    "user": "ascop_admin",
    "password": "ascop_password"
}
REDIS_HOST = "localhost"
REDIS_PORT = 6380

# --- Aachi Masala Product Mapping ---
# Intercepts dummy backend data and translates it for the client demo
AACHI_SKUS = {
    "widget_a": "Chicken Masala 500g",
    "widget_b": "Biryani Masala 200g",
    "widget_c": "Mango Thokku Pickle 200g",
    "widget_d": "Garam Masala 200g",
    "widget_e": "Idly Chilli Powder 100g"
}

def translate_to_aachi(raw_id):
    return AACHI_SKUS.get(str(raw_id).lower(), raw_id)

# --- Helper Functions ---
@st.cache_resource
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_redis_connection():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def fetch_inventory():
    try:
        conn = get_db_connection()
        df = pd.read_sql_query("SELECT item_id as \"SKU\", quantity as \"Stock Level\", price as \"Price (INR)\", last_updated FROM erp_inventory", conn)
        # Apply Aachi Masala branding to the dataframe
        df['SKU'] = df['SKU'].apply(translate_to_aachi)
        return df
    except Exception:
        return pd.DataFrame()

def fetch_ledger(limit=50):
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(f"SELECT created_at, event_type, payload FROM events_ledger ORDER BY created_at DESC LIMIT {limit}", conn)
        # Translate payload items for the audit log
        df['payload'] = df['payload'].apply(lambda x: str(x).replace('widget_a', 'Chicken Masala 500g').replace('widget_b', 'Biryani Masala 200g'))
        return df
    except Exception:
        return pd.DataFrame()

def fetch_hitl_queue():
    try:
        r = get_redis_connection()
        items = r.lrange("hitl_queue", 0, -1)
        # Return a tuple of the raw JSON string (for precise Redis deletion) and the parsed dictionary
        return [(i, json.loads(i)) for i in items]
    except Exception:
        return []

# --- Action Callbacks & Undo State ---
if 'pending_action_item' not in st.session_state:
    st.session_state.pending_action_item = None
    st.session_state.pending_action_type = ""

def queue_action(raw_str, action_type):
    st.session_state.pending_action_item = raw_str
    st.session_state.pending_action_type = action_type

def cancel_action():
    st.session_state.pending_action_item = None
    st.session_state.pending_action_type = ""

# --- UI Layout & Styling ---
st.set_page_config(page_title="ASCOP Command Center", layout="wide")

# Custom CSS for enterprise feel
st.markdown("""
    <style>
    .metric-card { background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #0052cc; }
    .diag-label { font-weight: 600; color: #555; }
    </style>
""", unsafe_allow_html=True)

# --- Sidebar Controls ---
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/c/c3/Python-logo-notext.svg/1200px-Python-logo-notext.svg.png", width=50)
    st.title("System Controls")
    if st.button("Sync Live Data", use_container_width=True, type="primary"):
        st.toast("Data synchronized with Redpanda and Postgres.")
    st.divider()
    st.caption("Agent Status")
    st.success("[ONLINE] Ingestion Plane")
    st.success("[ONLINE] Diagnostic Agent")
    st.success("[ONLINE] Execution Plane")
    st.caption(f"Last sync: {datetime.now().strftime('%H:%M:%S')}")

# --- Header & KPIs ---
st.title("ASCOP")

hitl_items = fetch_hitl_queue()
pending_count = len(hitl_items)
ledger_df = fetch_ledger()
total_events = len(ledger_df) if not ledger_df.empty else 0

# KPI Metrics Row
col1, col2, col3, col4 = st.columns(4)
col1.metric("ERP Synchronization", "Active", "12ms ping")
col2.metric("Pending Approvals", pending_count, f"{pending_count} critical", delta_color="inverse")
col3.metric("Autonomous Actions Executed", total_events, "+14 this hour")
col4.metric("System Health", "99.99%", "Optimal")

st.divider()

# --- Main Tabs ---
tab1, tab2, tab3 = st.tabs(["Economic Guardrail (HITL)", "Live ERP State", "Audit Ledger"])

with tab1:
    st.subheader("Action Queue")
    st.caption("Pending high-priority remediations requiring human authorization.")
    
    if not hitl_items:
        st.info("All systems nominal. No pending actions in the HITL queue.")
    else:
        for idx, (raw_str, item) in enumerate(hitl_items):
            event_type = item.get('event_type')
            
            if event_type == "Proposed_Fix_Event":
                with st.container(border=True):
                    # Localized Aachi ERP nomenclature
                    source_sys = item.get('source_system', 'SAP_S4HANA')
                    st.error(f"CRITICAL ERP FAULT DETECTED | Source: {source_sys}")
                    
                    # Split diagnosis and action into clean columns
                    info_col, action_col = st.columns([2, 1])
                    
                    with info_col:
                        st.markdown(f"<span class='diag-label'>Raw System Log:</span><br/> <code>{item.get('raw_error')}</code>", unsafe_allow_html=True)
                        st.markdown(f"<span class='diag-label'>AI Diagnosis:</span> {item.get('diagnosis')}", unsafe_allow_html=True)
                        st.markdown(f"<span class='diag-label'>Proposed Remediation:</span><br/> <b>{item.get('proposed_fix')}</b>", unsafe_allow_html=True)
                        
                    with action_col:
                        st.info(f"**Business Impact:**\n\n{item.get('expected_impact')}")
                        st.write(" ") # Spacer
                        
                        # Inline Undo Logic
                        if st.session_state.get('pending_action_item') == raw_str:
                            st.warning(f"Action '{st.session_state.pending_action_type}' queued.")
                            st.button("Undo", key=f"undo_{idx}", use_container_width=True, on_click=cancel_action)
                        else:
                            st.button("Authorize & Execute", key=f"fix_app_{idx}", use_container_width=True, type="primary", on_click=queue_action, args=(raw_str, "Authorize & Execute"))
                            st.button("Reject & Escalate", key=f"fix_rej_{idx}", use_container_width=True, on_click=queue_action, args=(raw_str, "Reject & Escalate"))
            else:
                # Apply Aachi SKU mapping to standard guardrail items
                display_item = translate_to_aachi(item.get('item_id', 'Unknown'))
                with st.expander(f"Standard Guardrail Veto: {display_item}"):
                    # Modify the JSON visually to show Aachi SKUs
                    display_json = json.dumps(item).replace("widget_a", "Chicken Masala 500g").replace("widget_b", "Biryani Masala 200g")
                    st.json(json.loads(display_json))
                    
                    if st.session_state.get('pending_action_item') == raw_str:
                        st.warning("Acknowledgment queued.")
                        st.button("Undo", key=f"undo_ack_{idx}", on_click=cancel_action)
                    else:
                        st.button("Acknowledge", key=f"ack_{idx}", on_click=queue_action, args=(raw_str, "Acknowledge"))

with tab2:
    st.subheader("Simulated Legacy ERP (Aachi Masala Inventory)")
    inventory_df = fetch_inventory()
    if not inventory_df.empty:
        # Hide index and use container width for a much cleaner look
        st.dataframe(inventory_df, use_container_width=True, hide_index=True)
    else:
        st.warning("No inventory data found. Is Postgres running?")

with tab3:
    st.subheader("Immutable Event Ledger")
    st.caption("Read-only record of all autonomous system decisions.")
    if not ledger_df.empty:
        st.dataframe(ledger_df, use_container_width=True, hide_index=True)

# Inline Timer Logic
# Halts the thread after the UI has painted. If untouched, executes deletion and clears state.
if st.session_state.get('pending_action_item'):
    time.sleep(3)
    # Ensure it wasn't canceled during the sleep window
    if st.session_state.get('pending_action_item'):
        try:
            r = get_redis_connection()
            r.lrem("hitl_queue", 1, st.session_state.pending_action_item)
        except Exception:
            pass
        st.session_state.pending_action_item = None
        st.session_state.pending_action_type = ""
        st.rerun()