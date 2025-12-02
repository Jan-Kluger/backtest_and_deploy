#!/usr/bin/env python3
"""
Import Binance BTCUSDT 1m kline CSV files into TimescaleDB
"""

import os
import glob
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# ============ CONFIGURATION ============
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "crypto",
    "user": "yannick",
    "password": "",
}

CSV_DIRECTORY = "./data"
TABLE_NAME = "btcusdt_1m"
BATCH_SIZE = 10000  # Insert rows in batches for performance
# =======================================


def create_table(conn):
    """Create the hypertable for kline data"""
    with conn.cursor() as cur:
        # Enable TimescaleDB extension
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        conn.commit()
        print("✓ TimescaleDB extension enabled")
        
        # Create the table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                open_time TIMESTAMPTZ NOT NULL,
                open NUMERIC(20, 8) NOT NULL,
                high NUMERIC(20, 8) NOT NULL,
                low NUMERIC(20, 8) NOT NULL,
                close NUMERIC(20, 8) NOT NULL,
                volume NUMERIC(30, 8) NOT NULL,
                close_time TIMESTAMPTZ NOT NULL,
                quote_asset_volume NUMERIC(30, 8) NOT NULL,
                number_of_trades INTEGER NOT NULL,
                taker_buy_base_volume NUMERIC(30, 8) NOT NULL,
                taker_buy_quote_volume NUMERIC(30, 8) NOT NULL,
                PRIMARY KEY (open_time)
            );
        """)
        
        # Convert to TimescaleDB hypertable (if not already)
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM timescaledb_information.hypertables 
                WHERE hypertable_name = '{TABLE_NAME}'
            );
        """)
        is_hypertable = cur.fetchone()[0]
        
        if not is_hypertable:
            cur.execute(f"""
                SELECT create_hypertable('{TABLE_NAME}', 'open_time', 
                    chunk_time_interval => INTERVAL '1 month',
                    if_not_exists => TRUE
                );
            """)
            print(f"✓ Created hypertable '{TABLE_NAME}'")
        else:
            print(f"✓ Hypertable '{TABLE_NAME}' already exists")
        
        conn.commit()


def parse_csv_row(row):
    """Parse a CSV row into database values"""
    parts = row.strip().split(',')
    if len(parts) != 12:
        return None
    
    # Convert milliseconds timestamp to datetime
    open_time = datetime.utcfromtimestamp(int(parts[0]) / 1000)
    close_time = datetime.utcfromtimestamp(int(parts[6]) / 1000)
    
    return (
        open_time,           # open_time
        parts[1],            # open
        parts[2],            # high
        parts[3],            # low
        parts[4],            # close
        parts[5],            # volume
        close_time,          # close_time
        parts[7],            # quote_asset_volume
        int(parts[8]),       # number_of_trades
        parts[9],            # taker_buy_base_volume
        parts[10],           # taker_buy_quote_volume
    )


def import_csv_file(conn, filepath):
    """Import a single CSV file"""
    rows = []
    
    with open(filepath, 'r') as f:
        for line in f:
            parsed = parse_csv_row(line)
            if parsed:
                rows.append(parsed)
    
    if not rows:
        return 0
    
    with conn.cursor() as cur:
        # Use execute_values for fast batch insert with ON CONFLICT to handle duplicates
        execute_values(
            cur,
            f"""
            INSERT INTO {TABLE_NAME} 
                (open_time, open, high, low, close, volume, close_time, 
                 quote_asset_volume, number_of_trades, taker_buy_base_volume, 
                 taker_buy_quote_volume)
            VALUES %s
            ON CONFLICT (open_time) DO NOTHING
            """,
            rows,
            page_size=BATCH_SIZE
        )
        conn.commit()
    
    return len(rows)


def main():
    print("=" * 50)
    print("Binance Kline CSV → TimescaleDB Importer")
    print("=" * 50)
    
    # Connect to database
    print(f"\nConnecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected to database")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        print("\n→ Make sure to edit DB_CONFIG at the top of this script!")
        return
    
    # Create table
    print("\nSetting up table...")
    try:
        create_table(conn)
    except Exception as e:
        print(f"✗ Failed to create table: {e}")
        conn.close()
        return
    
    # Find all CSV files
    csv_pattern = os.path.join(CSV_DIRECTORY, "*.csv")
    csv_files = sorted(glob.glob(csv_pattern))
    
    if not csv_files:
        print(f"\n✗ No CSV files found in {CSV_DIRECTORY}")
        conn.close()
        return
    
    print(f"\nFound {len(csv_files)} CSV files to import")
    print("-" * 50)
    
    # Import each file
    total_rows = 0
    for i, filepath in enumerate(csv_files, 1):
        filename = os.path.basename(filepath)
        try:
            rows = import_csv_file(conn, filepath)
            total_rows += rows
            print(f"[{i:4d}/{len(csv_files)}] {filename}: {rows:,} rows")
        except Exception as e:
            print(f"[{i:4d}/{len(csv_files)}] {filename}: ERROR - {e}")
    
    conn.close()
    
    print("-" * 50)
    print(f"\n✓ Import complete! Total rows: {total_rows:,}")
    print(f"\nQuery your data with:")
    print(f"  SELECT * FROM {TABLE_NAME} ORDER BY open_time DESC LIMIT 10;")
    print(f"\nExample aggregation (hourly OHLCV):")
    print(f"""  SELECT 
    time_bucket('1 hour', open_time) AS bucket,
    first(open, open_time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, open_time) AS close,
    sum(volume) AS volume
  FROM {TABLE_NAME}
  GROUP BY bucket
  ORDER BY bucket DESC
  LIMIT 24;""")


if __name__ == "__main__":
    main()

