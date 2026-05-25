import psycopg2
import time

def init_db():
    conn = None
    while True:
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5433,
                database="ascop_ledger",
                user="ascop_admin",
                password="ascop_password"
            )
            break
        except Exception as e:
            print(f"Waiting for Postgres... {e}")
            time.sleep(2)

    cur = conn.cursor()
    
    # Ledger table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events_ledger (
            id SERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Mock ERP Inventory table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS erp_inventory (
            item_id TEXT PRIMARY KEY,
            quantity INTEGER NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Seed some data
    cur.execute("INSERT INTO erp_inventory (item_id, quantity, price) VALUES ('widget_a', 100, 50.00) ON CONFLICT DO NOTHING;")
    cur.execute("INSERT INTO erp_inventory (item_id, quantity, price) VALUES ('widget_b', 20, 150.00) ON CONFLICT DO NOTHING;")

    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized.")

if __name__ == "__main__":
    init_db()
