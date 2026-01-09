#!/usr/bin/env python3
"""
Download Binance USD-M Futures data for specified symbols

Usage:
    python get_data.py BTCUSDT
    python get_data.py BTCUSDT --start 2024-01-01 --end 2024-12-31
    python get_data.py BTCUSDT ETHUSDT --months 3
"""

import os
import re
import sys
import glob
import argparse
import zipfile
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============ CONFIGURATION ============
INTERVAL = "1m"
BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
BASE_DATA_DIR = "./data"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
CHUNK = 1024 * 1024

# Datasets to download
DATASETS = {
    "klines": "klines",
    "aggTrades": "aggTrades",
    "bookDepth": "bookDepth",
    "markPriceKlines": "markPriceKlines",
}
# =======================================


def get_s3_prefix(dataset_name, symbol):
    """Build S3 prefix for a dataset and symbol"""
    if dataset_name in ["klines", "markPriceKlines"]:
        return f"data/futures/um/daily/{dataset_name}/{symbol}/{INTERVAL}/"
    else:
        return f"data/futures/um/daily/{dataset_name}/{symbol}/"


def get_symbol_data_dir(symbol, dataset_name):
    """Get local directory for a symbol and dataset: data/SYMBOL/dataset_name/"""
    return os.path.join(BASE_DATA_DIR, symbol, dataset_name)


def get_latest_csv_date(symbol, dataset_name):
    """Get the latest date from existing CSV files"""
    data_dir = get_symbol_data_dir(symbol, dataset_name)
    if not os.path.exists(data_dir):
        return None
    
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csv_files:
        return None
    
    dates = []
    for f in csv_files:
        match = re.search(r'(\d{4}-\d{2}-\d{2})\.csv$', f)
        if match:
            dates.append(datetime.strptime(match.group(1), '%Y-%m-%d').date())
    
    return max(dates) if dates else None


def list_remote_files(prefix):
    """List all .zip files from Binance S3 for a given prefix"""
    files = []
    marker = ""
    
    while True:
        url = f"{BASE}/?prefix={prefix}&delimiter=/"
        if marker:
            url += f"&marker={marker}"
        
        resp = requests.get(url, timeout=30)
        root = ET.fromstring(resp.text)
        
        contents_list = root.findall(f"{NS}Contents")
        for contents in contents_list:
            key = contents.find(f"{NS}Key").text
            if key.endswith(".zip"):
                files.append(key)
        
        is_truncated = root.find(f"{NS}IsTruncated").text == "true"
        if not is_truncated:
            break
        
        next_marker_node = root.find(f"{NS}NextMarker")
        if next_marker_node is not None and next_marker_node.text:
            marker = next_marker_node.text
        else:
            marker = contents_list[-1].find(f"{NS}Key").text
    
    return files


def extract_date_from_key(key):
    """Extract date from S3 key"""
    match = re.search(r'(\d{4}-\d{2}-\d{2})\.zip$', key)
    if match:
        return datetime.strptime(match.group(1), '%Y-%m-%d').date()
    return None


def download_and_extract(key, output_dir):
    """Download a zip file and extract the CSV to output_dir"""
    os.makedirs(output_dir, exist_ok=True)
    
    filename = key.split("/")[-1]
    zip_path = os.path.join(output_dir, filename)
    csv_filename = filename.replace(".zip", ".csv")
    csv_path = os.path.join(output_dir, csv_filename)
    url = f"{BASE}/{key}"
    
    # Skip if CSV already exists
    if os.path.exists(csv_path):
        return "skip"
    
    try:
        with requests.get(url, stream=True, timeout=(10, 300)) as r:
            if r.status_code != 200:
                return "error"
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=CHUNK):
                    if chunk:
                        f.write(chunk)
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(output_dir)
        
        return "ok"
    except Exception:
        return "error"
    finally:
        if os.path.exists(zip_path):
            os.remove(zip_path)


def process_symbol_dataset(symbol, dataset_name, start_date, end_date):
    """Process a single dataset for a single symbol"""
    output_dir = get_symbol_data_dir(symbol, dataset_name)
    prefix = get_s3_prefix(dataset_name, symbol)
    
    # Get latest CSV date
    latest_csv = get_latest_csv_date(symbol, dataset_name)
    
    # List remote files
    remote_files = list_remote_files(prefix)
    
    # Filter: within date range and newer than local data
    new_files = []
    for key in remote_files:
        file_date = extract_date_from_key(key)
        if file_date and start_date <= file_date <= end_date:
            if latest_csv and file_date <= latest_csv:
                continue
            new_files.append(key)
    
    if not new_files:
        return 0, latest_csv
    
    # Download files in parallel
    success_count = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_and_extract, key, output_dir): key 
                   for key in sorted(new_files)}
        for future in as_completed(futures):
            result = future.result()
            if result == "ok":
                success_count += 1
    
    return success_count, latest_csv


def process_symbol(symbol, start_date, end_date):
    """Process all datasets for a symbol"""
    print(f"\n{'='*60}")
    print(f"Symbol: {symbol}")
    print(f"Date range: {start_date} → {end_date}")
    print(f"{'='*60}")
    
    total_downloaded = 0
    
    for dataset_name in DATASETS.keys():
        downloaded, latest = process_symbol_dataset(symbol, dataset_name, start_date, end_date)
        total_downloaded += downloaded
        
        if latest:
            status = f"latest: {latest}"
        else:
            status = "no local data"
        
        if downloaded > 0:
            print(f"  {dataset_name}: +{downloaded} files ({status})")
        else:
            print(f"  {dataset_name}: up to date ({status})")
    
    return total_downloaded


def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Download Binance USD-M Futures data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python get_data.py BTCUSDT
  python get_data.py BTCUSDT ETHUSDT
  python get_data.py BTCUSDT --start 2024-01-01 --end 2024-12-31
  python get_data.py BTCUSDT --months 3
        """
    )
    
    parser.add_argument(
        'symbols',
        nargs='+',
        help='Symbol(s) to download (e.g., BTCUSDT ETHUSDT)'
    )
    
    parser.add_argument(
        '--start',
        type=str,
        help='Start date (YYYY-MM-DD). Default: 6 months ago'
    )
    
    parser.add_argument(
        '--end',
        type=str,
        help='End date (YYYY-MM-DD). Default: today'
    )
    
    parser.add_argument(
        '--months',
        type=int,
        default=6,
        help='Number of months to download (if --start not specified). Default: 6'
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Parse date range
    end_date = datetime.now().date()
    if args.end:
        end_date = datetime.strptime(args.end, '%Y-%m-%d').date()
    
    if args.start:
        start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    else:
        start_date = (datetime.now() - timedelta(days=args.months * 30)).date()
    
    if start_date > end_date:
        print("Error: start date must be before end date")
        sys.exit(1)
    
    print("=" * 60)
    print("  Binance USD-M Futures Data Downloader")
    print(f"  Symbols: {', '.join(args.symbols)}")
    print("=" * 60)
    
    total_new = 0
    
    # Process each symbol
    for symbol in args.symbols:
        symbol = symbol.upper()
        new_count = process_symbol(symbol, start_date, end_date)
        total_new += new_count
    
    # Summary
    print(f"\n{'='*60}")
    print(f"  Summary: {total_new} total new files downloaded")
    print(f"{'='*60}")
    
    if total_new > 0:
        print("\n✓ Download complete! Run 'python3 import_data.py' to import into database.")


if __name__ == "__main__":
    main()
