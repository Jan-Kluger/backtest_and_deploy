#!/usr/bin/env python3
"""
Import Binance BTCUSDT USD-M Futures data into TimescaleDB

Supports all dataset types with optimized compression:
- klines (1m candles)
- trades
- bookTicker
- markPriceKlines
- indexPriceKlines
- premiumIndexKlines
- depth (order book snapshots)
"""

import os
import re
import glob
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ============ CONFIGURATION ============
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ctrade",
    "user": "yannick",
    "password": "",
}

BASE_DATA_DIR = "./data"
BATCH_SIZE = 10000
PARALLEL_FILES = 4  # Number of files to import in parallel
# =======================================

# Dataset definitions: table name, schema, parser, chunk interval
# Only essential datasets for backtesting
DATASETS = {
    "klines": {
        "table": "btcusdt_1m",
        "columns": """
            ts TIMESTAMPTZ NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION NOT NULL,
            quote_volume DOUBLE PRECISION NOT NULL,
            trades INTEGER NOT NULL,
            taker_buy_volume DOUBLE PRECISION NOT NULL,
            taker_buy_quote_volume DOUBLE PRECISION NOT NULL
        """,
        "insert_cols": "ts, open, high, low, close, volume, quote_volume, trades, taker_buy_volume, taker_buy_quote_volume",
        "chunk_interval": "1 week",
        "compress_segmentby": "",
        "has_header": False,
    },
    "aggTrades": {
        "table": "btcusdt_aggtrades",
        "columns": """
            ts TIMESTAMPTZ NOT NULL,
            agg_trade_id BIGINT NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            qty DOUBLE PRECISION NOT NULL,
            first_trade_id BIGINT NOT NULL,
            last_trade_id BIGINT NOT NULL,
            is_buyer_maker BOOLEAN NOT NULL
        """,
        "insert_cols": "ts, agg_trade_id, price, qty, first_trade_id, last_trade_id, is_buyer_maker",
        "chunk_interval": "1 day",
        "compress_segmentby": "",
        "has_header": False,
    },
    "bookDepth": {
        "table": "btcusdt_depth",
        "columns": """
            ts TIMESTAMPTZ NOT NULL,
            percentage SMALLINT NOT NULL,
            depth DOUBLE PRECISION NOT NULL,
            notional DOUBLE PRECISION NOT NULL
        """,
        "insert_cols": "ts, percentage, depth, notional",
        "chunk_interval": "1 day",
        "compress_segmentby": "",
        "has_header": True,
    },
    "markPriceKlines": {
        "table": "btcusdt_markprice_1m",
        "columns": """
            ts TIMESTAMPTZ NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL
        """,
        "insert_cols": "ts, open, high, low, close",
        "chunk_interval": "1 week",
        "compress_segmentby": "",
        "has_header": False,
    },
}


def parse_timestamp(ts_str):
    """Parse timestamp - handles both ms (13 digits) and us (16 digits)"""
    ts_int = int(ts_str)
    divisor = 1_000_000 if ts_int > 9_999_999_999_999 else 1_000
    return datetime.utcfromtimestamp(ts_int / divisor)


def parse_klines_row(parts):
    """Parse klines CSV row"""
    if len(parts) < 11:
        return None
    # Skip header row (newer files have headers)
    if not parts[0].isdigit():
        return None
    return (
        parse_timestamp(parts[0]),  # ts (open time)
        float(parts[1]),            # open
        float(parts[2]),            # high
        float(parts[3]),            # low
        float(parts[4]),            # close
        float(parts[5]),            # volume
        float(parts[7]),            # quote_volume
        int(parts[8]),              # trades
        float(parts[9]),            # taker_buy_volume
        float(parts[10]),           # taker_buy_quote_volume
    )


def parse_aggtrades_row(parts):
    """Parse aggTrades CSV row
    Format: agg_trade_id, price, qty, first_trade_id, last_trade_id, timestamp, is_buyer_maker
    """
    if len(parts) != 7:
        return None
    # Skip header row
    if not parts[0].isdigit():
        return None
    return (
        parse_timestamp(parts[5]),           # ts
        int(parts[0]),                       # agg_trade_id
        float(parts[1]),                     # price
        float(parts[2]),                     # qty
        int(parts[3]),                       # first_trade_id
        int(parts[4]),                       # last_trade_id
        parts[6].strip().lower() == 'true',  # is_buyer_maker
    )


def parse_markprice_klines_row(parts):
    """Parse markPriceKlines/indexPriceKlines/premiumIndexKlines CSV row"""
    if len(parts) < 5:
        return None
    # Skip header row
    if not parts[0].isdigit():
        return None
    return (
        parse_timestamp(parts[0]),  # ts
        float(parts[1]),            # open
        float(parts[2]),            # high
        float(parts[3]),            # low
        float(parts[4]),            # close
    )


def parse_bookdepth_row(parts):
    """Parse bookDepth CSV row
    Format: timestamp, percentage, depth, notional
    Example: 2025-12-02 00:00:11,-5,4381.37200000,369958706.27690000
    """
    if len(parts) != 4:
        return None
    # Skip header row
    if parts[0] == 'timestamp':
        return None
    try:
        # Parse datetime string like "2025-12-02 00:00:11"
        ts = datetime.strptime(parts[0].strip(), '%Y-%m-%d %H:%M:%S')
        percentage = int(parts[1])
        depth = float(parts[2])
        notional = float(parts[3])
        return (ts, percentage, depth, notional)
    except:
        return None


PARSERS = {
    "klines": parse_klines_row,
    "aggTrades": parse_aggtrades_row,
    "bookDepth": parse_bookdepth_row,
    "markPriceKlines": parse_markprice_klines_row,
}


def create_table(conn, dataset_name):
    """Create hypertable with compression for a dataset"""
    config = DATASETS[dataset_name]
    table = config["table"]
    
    with conn.cursor() as cur:
        # Enable TimescaleDB
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        
        # Check if table exists
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = '{table}'
            );
        """)
        if cur.fetchone()[0]:
            conn.commit()
            return False  # Already exists
        
        # Create table
        cur.execute(f"CREATE TABLE {table} ({config['columns']});")
        
        # Convert to hypertable
        cur.execute(f"""
            SELECT create_hypertable('{table}', 'ts',
                chunk_time_interval => INTERVAL '{config['chunk_interval']}'
            );
        """)
        
        # Create index
        cur.execute(f"CREATE INDEX IF NOT EXISTS {table}_ts_idx ON {table} (ts DESC);")
        
        # Enable compression
        segmentby = config.get("compress_segmentby", "")
        if segmentby:
            cur.execute(f"""
                ALTER TABLE {table} SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = '{segmentby}',
                    timescaledb.compress_orderby = 'ts'
                );
            """)
        else:
            cur.execute(f"""
                ALTER TABLE {table} SET (
                    timescaledb.compress,
                    timescaledb.compress_orderby = 'ts'
                );
            """)
        
        # Add compression policy
        cur.execute(f"SELECT add_compression_policy('{table}', INTERVAL '7 days');")
        
        conn.commit()
        return True  # Created new


def get_latest_date(conn, table):
    """Get latest date in table"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(ts)::date FROM {table};")
        result = cur.fetchone()[0]
        return result


def extract_date_from_filename(filename):
    """Extract date from filename"""
    match = re.search(r'(\d{4}-\d{2}-\d{2})\.csv$', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y-%m-%d').date()
    return None


def import_csv_file(conn, filepath, dataset_name):
    """Import a single CSV file"""
    config = DATASETS[dataset_name]
    parser = PARSERS[dataset_name]
    has_header = config.get("has_header", False)
    
    rows = []
    with open(filepath, 'r') as f:
        for i, line in enumerate(f):
            if has_header and i == 0:
                continue
            parts = line.strip().split(',')
            parsed = parser(parts)
            if parsed:
                rows.append(parsed)
    
    if not rows:
        return 0
    
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {config['table']} ({config['insert_cols']})
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
            page_size=BATCH_SIZE
        )
        conn.commit()
    
    return len(rows)


def get_compression_stats(conn, table):
    """Get compression statistics"""
    with conn.cursor() as cur:
        try:
            cur.execute(f"""
                SELECT 
                    pg_size_pretty(before_compression_total_bytes) as before,
                    pg_size_pretty(after_compression_total_bytes) as after,
                    ROUND(100 - (after_compression_total_bytes::numeric / 
                        NULLIF(before_compression_total_bytes, 0) * 100), 1) as ratio
                FROM hypertable_compression_stats('{table}');
            """)
            result = cur.fetchone()
            if result and result[0]:
                return result
        except:
            pass
        return None


def process_dataset(dataset_name):
    """Process a single dataset"""
    config = DATASETS[dataset_name]
    table = config["table"]
    data_dir = os.path.join(BASE_DATA_DIR, dataset_name)
    
    print(f"\n{'='*50}")
    print(f"Dataset: {dataset_name} → {table}")
    print(f"{'='*50}")
    
    # Check if data directory exists
    if not os.path.exists(data_dir):
        print(f"⏭ No data directory: {data_dir}")
        return 0
    
    # Connect
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Create table if needed
    created = create_table(conn, dataset_name)
    if created:
        print(f"✓ Created table '{table}' with compression")
    else:
        print(f"✓ Table '{table}' exists")
    
    # Get latest date
    latest_date = get_latest_date(conn, table)
    if latest_date:
        print(f"✓ Latest data: {latest_date}")
    
    # Find CSV files
    csv_files = sorted(glob.glob(os.path.join(data_dir, "*.csv")))
    
    # Filter to new files
    if latest_date:
        csv_files = [f for f in csv_files 
                     if (d := extract_date_from_filename(f)) and d > latest_date]
    
    if not csv_files:
        print("✓ Already up to date")
        stats = get_compression_stats(conn, table)
        if stats:
            print(f"📊 Compression: {stats[0]} → {stats[1]} ({stats[2]}% reduction)")
        conn.close()
        return 0
    
    print(f"Importing {len(csv_files)} files...")
    print("-" * 50)
    
    # Import files
    total_rows = 0
    for i, filepath in enumerate(csv_files, 1):
        filename = os.path.basename(filepath)
        try:
            rows = import_csv_file(conn, filepath, dataset_name)
            total_rows += rows
            print(f"[{i:4d}/{len(csv_files)}] {filename}: {rows:,} rows")
        except Exception as e:
            print(f"[{i:4d}/{len(csv_files)}] {filename}: ERROR - {e}")
    
    print("-" * 50)
    print(f"✓ Imported {total_rows:,} rows")
    
    # Show compression stats
    stats = get_compression_stats(conn, table)
    if stats:
        print(f"📊 Compression: {stats[0]} → {stats[1]} ({stats[2]}% reduction)")
    
    conn.close()
    return total_rows


def main():
    print("=" * 60)
    print("  Binance BTCUSDT USD-M Futures → TimescaleDB")
    print("  All datasets with compression")
    print("=" * 60)
    
    # Test connection
    print(f"\nConnecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("✓ Connected to database")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        print("\n→ Make sure to edit DB_CONFIG at the top of this script!")
        return
    
    # Process each dataset
    grand_total = 0
    for dataset_name in DATASETS.keys():
        try:
            rows = process_dataset(dataset_name)
            grand_total += rows
        except Exception as e:
            print(f"✗ Failed to process {dataset_name}: {e}")
    
    print(f"\n{'='*60}")
    print(f"  Total rows imported: {grand_total:,}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

