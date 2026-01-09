#!/usr/bin/env python3
"""
Import Binance USD-M Futures data into TimescaleDB (Multi-Asset)

Unified tables with symbol column - one table per dataset type, not per symbol.
Supports: klines, aggTrades, bookDepth, markPriceKlines
"""

import os
import re
import glob
import io
import threading
import time
import psycopg2
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("⚠ Warning: pandas not installed. Install with: pip install pandas")

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
PARALLEL_WORKERS = 4  # Number of parallel insert threads
# =======================================

# Unified dataset definitions (one table per dataset type, symbol as column)
DATASETS = {
    "klines": {
        "table": "klines_1m",
        "columns": """
            ts TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION NOT NULL,
            quote_volume DOUBLE PRECISION NOT NULL,
            trades INTEGER NOT NULL,
            taker_buy_volume DOUBLE PRECISION NOT NULL,
            taker_buy_quote_volume DOUBLE PRECISION NOT NULL,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        """,
        "insert_cols": "ts, symbol, open, high, low, close, volume, quote_volume, trades, taker_buy_volume, taker_buy_quote_volume",
        "unique_constraint": "UNIQUE (ts, symbol)",
        "chunk_interval": "1 week",
        "compress_segmentby": "symbol",
    },
    "aggTrades": {
        "table": "aggtrades",
        "columns": """
            ts TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            agg_trade_id BIGINT NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            qty DOUBLE PRECISION NOT NULL,
            first_trade_id BIGINT NOT NULL,
            last_trade_id BIGINT NOT NULL,
            is_buyer_maker BOOLEAN NOT NULL,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        """,
        "insert_cols": "ts, symbol, agg_trade_id, price, qty, first_trade_id, last_trade_id, is_buyer_maker",
        "unique_constraint": "UNIQUE (ts, symbol, agg_trade_id)",
        "chunk_interval": "1 day",
        "compress_segmentby": "symbol",
    },
    "bookDepth": {
        "table": "book_depth",
        "columns": """
            ts TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            percentage SMALLINT NOT NULL,
            depth DOUBLE PRECISION NOT NULL,
            notional DOUBLE PRECISION NOT NULL,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        """,
        "insert_cols": "ts, symbol, percentage, depth, notional",
        "unique_constraint": "UNIQUE (ts, symbol, percentage)",
        "chunk_interval": "1 day",
        "compress_segmentby": "symbol",
    },
    "markPriceKlines": {
        "table": "markprice_klines_1m",
        "columns": """
            ts TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        """,
        "insert_cols": "ts, symbol, open, high, low, close",
        "unique_constraint": "UNIQUE (ts, symbol)",
        "chunk_interval": "1 week",
        "compress_segmentby": "symbol",
    },
}

# Engine-facing views (stable interface for backtesting engine)
ENGINE_VIEWS = {
    "klines_1m": """
        CREATE OR REPLACE VIEW engine_klines_1m AS
        SELECT
            ts,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trades,
            taker_buy_volume,
            taker_buy_quote_volume
        FROM klines_1m;
    """,
    "aggtrades": """
        CREATE OR REPLACE VIEW engine_aggtrades AS
        SELECT
            ts,
            symbol,
            agg_trade_id,
            price,
            qty,
            first_trade_id,
            last_trade_id,
            is_buyer_maker
        FROM aggtrades;
    """,
    "book_depth": """
        CREATE OR REPLACE VIEW engine_book_depth AS
        SELECT
            ts,
            symbol,
            percentage,
            depth,
            notional
        FROM book_depth;
    """,
    "markprice_klines_1m": """
        CREATE OR REPLACE VIEW engine_markprice_1m AS
        SELECT
            ts,
            symbol,
            open,
            high,
            low,
            close
        FROM markprice_klines_1m;
    """,
}


def extract_symbol_from_path(filepath):
    """Extract symbol from file path or filename.
    
    Examples:
        BTCUSDT-1m-2025-01-01.csv -> BTCUSDT
        ETHUSDT-aggTrades-2025-01-01.csv -> ETHUSDT
    """
    filename = os.path.basename(filepath)
    match = re.match(r'^([A-Z0-9]+)-', filename)
    if match:
        return match.group(1)
    return None


def parse_timestamp(ts_str):
    """Parse timestamp - handles both ms (13 digits) and us (16 digits)"""
    ts_int = int(ts_str)
    divisor = 1_000_000 if ts_int > 9_999_999_999_999 else 1_000
    return datetime.fromtimestamp(ts_int / divisor, tz=timezone.utc).replace(tzinfo=None)


def parse_klines_row(parts, symbol):
    """Parse klines CSV row"""
    if len(parts) < 11:
        return None
    if not parts[0].isdigit():
        return None
    return (
        parse_timestamp(parts[0]),  # ts
        symbol,                      # symbol
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


def parse_aggtrades_row(parts, symbol):
    """Parse aggTrades CSV row"""
    if len(parts) != 7:
        return None
    if not parts[0].isdigit():
        return None
    return (
        parse_timestamp(parts[5]),           # ts
        symbol,                              # symbol
        int(parts[0]),                       # agg_trade_id
        float(parts[1]),                     # price
        float(parts[2]),                     # qty
        int(parts[3]),                       # first_trade_id
        int(parts[4]),                       # last_trade_id
        parts[6].strip().lower() == 'true',  # is_buyer_maker
    )


def parse_bookdepth_row(parts, symbol):
    """Parse bookDepth CSV row"""
    if len(parts) != 4:
        return None
    if parts[0] == 'timestamp':
        return None
    try:
        ts = datetime.strptime(parts[0].strip(), '%Y-%m-%d %H:%M:%S')
        return (ts, symbol, int(parts[1]), float(parts[2]), float(parts[3]))
    except:
        return None


def parse_markprice_klines_row(parts, symbol):
    """Parse markPriceKlines CSV row"""
    if len(parts) < 5:
        return None
    if not parts[0].isdigit():
        return None
    return (
        parse_timestamp(parts[0]),  # ts
        symbol,                      # symbol
        float(parts[1]),            # open
        float(parts[2]),            # high
        float(parts[3]),            # low
        float(parts[4]),            # close
    )


PARSERS = {
    "klines": parse_klines_row,
    "aggTrades": parse_aggtrades_row,
    "bookDepth": parse_bookdepth_row,
    "markPriceKlines": parse_markprice_klines_row,
}


def create_table(conn, dataset_name):
    """Create unified hypertable with compression and unique constraints"""
    config = DATASETS[dataset_name]
    table = config["table"]
    
    with conn.cursor() as cur:
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
            return False
        
        # Create unified table with unique constraint
        columns = config['columns']
        unique = config.get('unique_constraint', '')
        if unique:
            cur.execute(f"CREATE TABLE {table} ({columns}, {unique});")
        else:
            cur.execute(f"CREATE TABLE {table} ({columns});")
        
        # Convert to hypertable
        cur.execute(f"""
            SELECT create_hypertable('{table}', 'ts',
                chunk_time_interval => INTERVAL '{config['chunk_interval']}'
            );
        """)
        
        # Create indexes
        cur.execute(f"CREATE INDEX IF NOT EXISTS {table}_ts_idx ON {table} (ts DESC);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {table}_symbol_ts_idx ON {table} (symbol, ts DESC);")
        
        # Enable compression with symbol segmentation
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
        return True


def create_engine_views(conn):
    """Create engine-facing views for stable API"""
    with conn.cursor() as cur:
        for view_name, view_sql in ENGINE_VIEWS.items():
            try:
                # Check if table exists before creating view
                table_name = view_sql.split('FROM ')[1].split(';')[0].strip()
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = '{table_name}'
                    );
                """)
                if cur.fetchone()[0]:
                    cur.execute(view_sql)
                    print(f"  ✓ engine_{view_name.replace('_1m', '').replace('_klines', '')}")
                else:
                    print(f"  ⏭ Skipping {view_name} (table {table_name} doesn't exist yet)")
            except Exception as e:
                print(f"  ⚠ {view_name}: {e}")
        conn.commit()


def get_latest_date_for_symbol(conn, table, symbol):
    """Get latest date in table for a specific symbol"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(ts)::date FROM {table} WHERE symbol = %s;", (symbol,))
        result = cur.fetchone()[0]
        return result


def extract_date_from_filename(filename):
    """Extract date from filename"""
    match = re.search(r'(\d{4}-\d{2}-\d{2})\.csv$', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y-%m-%d').date()
    return None


def discover_symbols():
    """Discover all symbols by looking at data/SYMBOL/ directories"""
    if not os.path.exists(BASE_DATA_DIR):
        return set()
    
    symbols = set()
    for item in os.listdir(BASE_DATA_DIR):
        symbol_dir = os.path.join(BASE_DATA_DIR, item)
        if os.path.isdir(symbol_dir) and item.isupper():
            symbols.add(item)
    
    return symbols


def get_dataset_dir_for_symbol(symbol, dataset_name):
    """Get directory for a specific symbol and dataset"""
    return os.path.join(BASE_DATA_DIR, symbol, dataset_name)


def import_csv_file(filepath, dataset_name):
    """Import a single CSV file using COPY FROM (fastest PostgreSQL bulk load)"""
    config = DATASETS[dataset_name]
    
    # Extract symbol from file path
    symbol = extract_symbol_from_path(filepath)
    if not symbol:
        return 0, None
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Use vectorized pandas processing for speed
        if HAS_PANDAS:
            df = pd.read_csv(filepath, header=None, dtype=str, engine='c', low_memory=False)
            
            if df.empty:
                conn.close()
                return 0, None
            
            # Vectorized parsing based on dataset type
            if dataset_name == "klines":
                # Filter out header rows and invalid rows
                df = df[df[0].str.isdigit().fillna(False)]
                if df.empty:
                    conn.close()
                    return 0, None
                    
                # Vectorized timestamp conversion
                ts_ms = df[0].astype(float)
                ts = pd.to_datetime(ts_ms, unit='ms', utc=True).dt.tz_localize(None)
                
                result = pd.DataFrame({
                    'ts': ts,
                    'symbol': symbol,
                    'open': df[1].astype(float),
                    'high': df[2].astype(float),
                    'low': df[3].astype(float),
                    'close': df[4].astype(float),
                    'volume': df[5].astype(float),
                    'quote_volume': df[7].astype(float),
                    'trades': df[8].astype(int),
                    'taker_buy_volume': df[9].astype(float),
                    'taker_buy_quote_volume': df[10].astype(float),
                })
                
            elif dataset_name == "aggTrades":
                df = df[df[0].str.isdigit().fillna(False)]
                if df.empty:
                    conn.close()
                    return 0, None
                
                # Column 5 is timestamp in ms
                ts_ms = df[5].astype(float)
                ts = pd.to_datetime(ts_ms, unit='ms', utc=True).dt.tz_localize(None)
                
                result = pd.DataFrame({
                    'ts': ts,
                    'symbol': symbol,
                    'agg_trade_id': df[0].astype('int64'),
                    'price': df[1].astype(float),
                    'qty': df[2].astype(float),
                    'first_trade_id': df[3].astype('int64'),
                    'last_trade_id': df[4].astype('int64'),
                    'is_buyer_maker': df[6].str.strip().str.lower() == 'true',
                })
                
            elif dataset_name == "bookDepth":
                # Skip header row if present
                df = df[df[0] != 'timestamp']
                if df.empty:
                    conn.close()
                    return 0, None
                
                ts = pd.to_datetime(df[0], format='%Y-%m-%d %H:%M:%S')
                
                result = pd.DataFrame({
                    'ts': ts,
                    'symbol': symbol,
                    'percentage': df[1].astype(int),
                    'depth': df[2].astype(float),
                    'notional': df[3].astype(float),
                })
                
            elif dataset_name == "markPriceKlines":
                df = df[df[0].str.isdigit().fillna(False)]
                if df.empty:
                    conn.close()
                    return 0, None
                
                ts_ms = df[0].astype(float)
                ts = pd.to_datetime(ts_ms, unit='ms', utc=True).dt.tz_localize(None)
                
                result = pd.DataFrame({
                    'ts': ts,
                    'symbol': symbol,
                    'open': df[1].astype(float),
                    'high': df[2].astype(float),
                    'low': df[3].astype(float),
                    'close': df[4].astype(float),
                })
            else:
                conn.close()
                return 0, "Unknown dataset type"
            
            if result.empty:
                conn.close()
                return 0, None
            
            # Convert booleans to PostgreSQL format
            for col in result.select_dtypes(include=['bool']).columns:
                result[col] = result[col].map({True: 't', False: 'f'})
            
            # Write to StringIO buffer using pandas (much faster than iterrows)
            buffer = io.StringIO()
            result.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N', 
                          date_format='%Y-%m-%d %H:%M:%S.%f')
            buffer.seek(0)
            row_count = len(result)
            
        else:
            # Fallback: line-by-line parsing
            parser = PARSERS[dataset_name]
            rows = []
            with open(filepath, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    parsed = parser(parts, symbol)
                    if parsed:
                        rows.append(parsed)
            
            if not rows:
                conn.close()
                return 0, None
            
            buffer = io.StringIO()
            for row in rows:
                formatted_values = []
                for val in row:
                    if isinstance(val, datetime):
                        formatted_values.append(val.isoformat())
                    elif isinstance(val, bool):
                        formatted_values.append('t' if val else 'f')
                    elif val is None:
                        formatted_values.append('\\N')
                    else:
                        s = str(val).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                        formatted_values.append(s)
                buffer.write('\t'.join(formatted_values) + '\n')
            
            buffer.seek(0)
            row_count = len(rows)
        
        # Use COPY FROM STDIN (fastest PostgreSQL bulk load method)
        with conn.cursor() as cur:
            # Unique temp table name per thread
            temp_table = f"temp_{config['table']}_{threading.get_ident()}"
            cur.execute(f"""
                CREATE TEMP TABLE IF NOT EXISTS {temp_table} (LIKE {config['table']} INCLUDING DEFAULTS);
                TRUNCATE {temp_table};
            """)
            
            columns = config['insert_cols'].split(', ')
            cur.copy_from(
                buffer,
                temp_table,
                columns=columns,
                sep='\t',
                null='\\N'
            )
            
            # Insert from temp table with ON CONFLICT DO NOTHING
            cur.execute(f"""
                INSERT INTO {config['table']} ({config['insert_cols']})
                SELECT {config['insert_cols']}
                FROM {temp_table}
                ON CONFLICT DO NOTHING;
            """)
            
            inserted_count = cur.rowcount
            conn.commit()
        
        conn.close()
        return inserted_count, None
        
    except Exception as e:
        return 0, str(e)


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
    """Process a single dataset across all discovered symbols"""
    config = DATASETS[dataset_name]
    table = config["table"]
    
    print(f"\n{'='*50}")
    print(f"Dataset: {dataset_name} → {table}")
    print(f"{'='*50}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Create table if needed
    created = create_table(conn, dataset_name)
    if created:
        print(f"✓ Created unified table '{table}'")
    else:
        print(f"✓ Table '{table}' exists")
    
    # Discover symbols
    symbols = discover_symbols()
    if not symbols:
        print("⏭ No symbols found in data directory")
        conn.close()
        return 0
    
    print(f"✓ Symbols found: {', '.join(sorted(symbols))}")
    
    # Show latest dates per symbol
    for symbol in sorted(symbols):
        latest = get_latest_date_for_symbol(conn, table, symbol)
        if latest:
            print(f"  {symbol}: latest data = {latest}")
    
    # Collect all CSV files across all symbols
    all_files = []
    for symbol in symbols:
        data_dir = get_dataset_dir_for_symbol(symbol, dataset_name)
        if os.path.exists(data_dir):
            csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
            all_files.extend(csv_files)
    
    if not all_files:
        print("✓ No CSV files to import")
        conn.close()
        return 0
    
    # Filter to files not yet imported
    # Use >= to re-import latest day (handles partial imports on resume)
    # ON CONFLICT DO NOTHING handles duplicates safely
    files_to_import = []
    skipped_count = 0
    for filepath in all_files:
        symbol = extract_symbol_from_path(filepath)
        file_date = extract_date_from_filename(os.path.basename(filepath))
        if symbol and file_date:
            latest = get_latest_date_for_symbol(conn, table, symbol)
            if not latest or file_date >= latest:
                files_to_import.append(filepath)
            else:
                skipped_count += 1
    
    if skipped_count > 0:
        print(f"⏭ Skipped {skipped_count} files (already in DB)")
    
    if not files_to_import:
        print("✓ Already up to date")
        stats = get_compression_stats(conn, table)
        if stats:
            print(f"📊 Compression: {stats[0]} → {stats[1]} ({stats[2]}% reduction)")
        conn.close()
        return 0
    
    # Close connection before parallel processing (each thread creates its own)
    conn.close()
    
    print(f"Importing {len(files_to_import)} files (using {PARALLEL_WORKERS} parallel workers)...")
    print("-" * 50, flush=True)
    
    # Import files in parallel
    total_rows = 0
    completed = 0
    errors = []
    start_time = time.time()
    
    # Use a lock for thread-safe printing
    print_lock = threading.Lock()
    
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(import_csv_file, filepath, dataset_name): filepath 
                   for filepath in files_to_import}
        
        for future in as_completed(futures):
            filepath = futures[future]
            filename = os.path.basename(filepath)
            completed += 1
            elapsed = time.time() - start_time
            
            try:
                rows, error = future.result()
                with print_lock:
                    if error:
                        errors.append((filename, error))
                        print(f"[{completed:4d}/{len(files_to_import)}] {filename}: ERROR - {error}", flush=True)
                    elif rows > 0:
                        total_rows += rows
                        rate = completed / elapsed if elapsed > 0 else 0
                        print(f"[{completed:4d}/{len(files_to_import)}] {filename}: {rows:,} rows ({rate:.1f} files/sec)", flush=True)
            except Exception as e:
                with print_lock:
                    errors.append((filename, str(e)))
                    print(f"[{completed:4d}/{len(files_to_import)}] {filename}: ERROR - {e}", flush=True)
    
    elapsed = time.time() - start_time
    print("-" * 50)
    print(f"✓ Imported {total_rows:,} rows in {elapsed:.1f}s ({len(files_to_import)/elapsed:.1f} files/sec)")
    if errors:
        print(f"⚠ {len(errors)} files had errors")
    
    # Get compression stats (need new connection since we closed the old one)
    conn = psycopg2.connect(**DB_CONFIG)
    stats = get_compression_stats(conn, table)
    if stats:
        print(f"📊 Compression: {stats[0]} → {stats[1]} ({stats[2]}% reduction)")
    conn.close()
    
    return total_rows


def main():
    print("=" * 60)
    print("  Binance USD-M Futures → TimescaleDB (Multi-Asset)")
    print("  Unified tables with symbol column")
    print("=" * 60)
    
    # Test connection
    print(f"\nConnecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("✓ Connected to database")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return
    
    # Process each dataset
    grand_total = 0
    for dataset_name in DATASETS.keys():
        try:
            rows = process_dataset(dataset_name)
            grand_total += rows
        except Exception as e:
            print(f"✗ Failed to process {dataset_name}: {e}")
    
    # Create engine views
    print(f"\n{'='*50}")
    print("Creating engine-facing views...")
    print(f"{'='*50}")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        create_engine_views(conn)
        conn.close()
    except Exception as e:
        print(f"⚠ View creation failed: {e}")
    
    print(f"\n{'='*60}")
    print(f"  Total rows imported: {grand_total:,}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
