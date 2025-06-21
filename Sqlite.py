import sqlite3
import pandas as pd
import datetime
import gc
import time
import traceback

# ---------------------------- #
# 1. CREATE OR CONNECT SQLITE DB
# ---------------------------- #
def create_or_connect_sqlite_db(sql_local_db_path):
    """
    Creates or connects to a local SQLite DB with optimized settings for performance.
    """
    conn = sqlite3.connect(sql_local_db_path, isolation_level=None)
    conn.execute('PRAGMA journal_mode = OFF;')
    conn.execute('PRAGMA synchronous = 0;')
    conn.execute('PRAGMA locking_mode = SHARED')
    conn.execute('PRAGMA busy_timeout = 60000;')
    conn.execute('PRAGMA cache_size = 10000000;')
    conn.execute('PRAGMA temp_store = MEMORY;')
    conn.execute('PRAGMA read_uncommitted = 1;')
    conn.execute('PRAGMA mmap_size=268435456;')
    conn.commit()
    print("✅ SQLite DB connection established.")
    return conn

# ---------------------------- #
# 2. CREATE TABLE IF NOT EXISTS
# ---------------------------- #
def create_sql_table(conn, table_name):
    """
    Creates the target table in the SQLite DB if it does not already exist.
    """
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            vin TEXT,
            deviceId TEXT,
            rule_status TEXT,
            notification_timestamp TEXT,
            spn_fmi_counter TEXT,
            notifyflag TEXT,
            realtime_status TEXT,
            measurementstarttimestamp TEXT,
            partition_path TEXT,
            currenttimestamp TEXT,
            spn_fmi TEXT,
            unique_id TEXT,
            kafka_offset TEXT,
            kafka_partition TEXT,
            error TEXT,
            toggle TEXT,
            dealer_name TEXT,
            dealer_location TEXT,
            geofence_date_timestamp TEXT,
            vehicle_position_date_timestamp TEXT,
            sent_message_date_time_stamp TEXT,
            vehicle_latitude TEXT,
            vehicle_longitude TEXT,
            pci_check TEXT
        )
    ''')
    conn.commit()
    print(f"✅ Table `{table_name}` ready in SQLite DB.")

# ---------------------------- #
# 3. INSERT RECORDS TO TABLE
# ---------------------------- #
def insert_records(conn, table_name, records):
    """
    Inserts a list of dictionaries (records) into the specified SQLite table.
    """
    try:
        df = pd.DataFrame.from_records(records).applymap(str)
        insert_query = f'''
            INSERT INTO {table_name} (
                vin, deviceId, rule_status, notification_timestamp, spn_fmi_counter, notifyflag,
                realtime_status, measurementstarttimestamp, partition_path, currenttimestamp,
                spn_fmi, unique_id, kafka_offset, kafka_partition, error, toggle,
                dealer_name, dealer_location, geofence_date_timestamp, vehicle_position_date_timestamp,
                sent_message_date_time_stamp, vehicle_latitude, vehicle_longitude, pci_check
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        conn.executemany(insert_query, df.to_records(index=False))
        conn.commit()
        print(f"✅ {len(records)} records inserted into `{table_name}`.")
    except Exception as e:
        print("❌ Error while inserting records:", e)
        traceback.print_exc()

# ---------------------------- #
# 4. READ FROM TABLE
# ---------------------------- #
def read_sqlite_table(conn, table_name):
    """
    Reads all records from the SQLite table and returns as a DataFrame.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    records = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(records, columns=columns)

# ---------------------------- #
# 5. CLEAN OLD RECORDS
# ---------------------------- #
def cleanup_sqlite(conn, table_name, timestamp_column='currenttimestamp', last_processed_timestamp=None):
    """
    Deletes processed records from the SQLite DB based on timestamp.
    """
    cursor = conn.cursor()
    cursor.execute(f"""
        DELETE FROM {table_name}
        WHERE {timestamp_column} <= ?
    """, (last_processed_timestamp,))
    cursor.execute("VACUUM")
    conn.commit()
    print(f"🧹 Cleaned records older than {last_processed_timestamp}.")

# ---------------------------- #
# 6. GENERATE UNIQUE ID
# ---------------------------- #
def generate_unique_number():
    """
    Generates a timestamp-based unique ID.
    """
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1]

# ---------------------------- #
# ✅ USAGE EXAMPLE
# ---------------------------- #
if __name__ == "__main__":
    db_path = "/tmp/mydata.db"
    table = "clogged_air_filter"

    # Connect to DB
    conn = create_or_connect_sqlite_db(db_path)

    # Create table
    create_sql_table(conn, table)

    # Sample insert
    records = [{
        "vin": "123ABC",
        "deviceId": "DEVICE01",
        "rule_status": "TRIGGERED",
        "notification_timestamp": "2024-01-01T00:00:00Z",
        "spn_fmi_counter": "3",
        "notifyflag": "Y",
        "realtime_status": "ONLINE",
        "measurementstarttimestamp": "1720000000000",
        "partition_path": "/path",
        "currenttimestamp": datetime.datetime.now().isoformat(),
        "spn_fmi": "5086-18",
        "unique_id": generate_unique_number(),
        "kafka_offset": "456",
        "kafka_partition": "1",
        "error": "",
        "toggle": "ON",
        "dealer_name": "DealerX",
        "dealer_location": "Bangalore",
        "geofence_date_timestamp": "2024-01-01T00:00:00Z",
        "vehicle_position_date_timestamp": "2024-01-01T00:00:00Z",
        "sent_message_date_time_stamp": "2024-01-01T00:00:00Z",
        "vehicle_latitude": "12.9716",
        "vehicle_longitude": "77.5946",
        "pci_check": "PASS"
    }]

    # Insert records
    insert_records(conn, table, records)

    # Read data
    df = read_sqlite_table(conn, table)
    print(df.head())

    # Cleanup
    last_timestamp = df['currenttimestamp'].max()
    cleanup_sqlite(conn, table, last_processed_timestamp=last_timestamp)

    conn.close()
