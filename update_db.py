#!/usr/bin/env python3
"""
Download BTCUSDT USD-M futures data from Binance and import into TimescaleDB
"""

import os
import re
import glob
import zipfile
import subprocess
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============ CONFIGURATION ============
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
BASE_DATA_DIR = "./data"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
CHUNK = 1024 * 1024

# Only download data from the last N months
MONTHS_OF_DATA = 6
MIN_DATE = (datetime.now() - timedelta(days=MONTHS_OF_DATA * 30)).date()

# Essential datasets for backtesting (can't be derived from other data)
DATASETS = {
    "klines": f"data/futures/um/daily/klines/{SYMBOL}/{INTERVAL}/",
    "aggTrades": f"data/futures/um/daily/aggTrades/{SYMBOL}/",
    "bookDepth": f"data/futures/um/daily/bookDepth/{SYMBOL}/",
    "markPriceKlines": f"data/futures/um/daily/markPriceKlines/{SYMBOL}/{INTERVAL}/",
}
# =======================================


def get_dataset_dir(dataset_name):
    """Get local directory for a dataset"""
    return os.path.join(BASE_DATA_DIR, dataset_name)


def get_latest_csv_date(dataset_name):
    """Get the latest date from existing CSV files for a dataset"""
    data_dir = get_dataset_dir(dataset_name)
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
    """Extract date from S3 key like .../BTCUSDT-1m-2025-01-01.zip"""
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
                print(f"    [ERROR] status {r.status_code}")
                return "error"
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=CHUNK):
                    if chunk:
                        f.write(chunk)
        
        # Extract CSV from zip
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(output_dir)
        
        return "ok"
        
    except Exception as e:
        print(f"    [FAILED] {filename}: {e}")
        return "error"
    finally:
        # Always clean up zip file
        if os.path.exists(zip_path):
            os.remove(zip_path)


def process_dataset(dataset_name, prefix):
    """Process a single dataset: list, filter, download"""
    print(f"\n{'='*50}")
    print(f"Dataset: {dataset_name}")
    print(f"{'='*50}")
    
    output_dir = get_dataset_dir(dataset_name)
    
    # Get latest date from CSV files only
    latest_csv = get_latest_csv_date(dataset_name)
    
    if latest_csv:
        print(f"✓ Latest CSV: {latest_csv}")
    print(f"✓ Only fetching data from: {MIN_DATE} ({MONTHS_OF_DATA} months)")
    
    # List remote files
    print(f"Fetching file list from S3...")
    remote_files = list_remote_files(prefix)
    print(f"✓ Found {len(remote_files)} files on Binance")
    
    # Filter: must be >= MIN_DATE and newer than what we have locally
    new_files = []
    all_dates = []
    for key in remote_files:
        file_date = extract_date_from_key(key)
        if file_date:
            all_dates.append(file_date)
        if file_date and file_date >= MIN_DATE:
            # Skip if we already have this file
            if latest_csv and file_date <= latest_csv:
                continue
            new_files.append(key)
    
    # Debug: show date range found
    if all_dates:
        print(f"  (Remote files range: {min(all_dates)} to {max(all_dates)})")
    
    if not new_files:
        print("✓ Already up to date!")
        return 0
    
    print(f"{len(new_files)} new files to download")
    print("-" * 50)
    
    # Download and extract new files in parallel
    success_count = 0
    skip_count = 0
    error_count = 0
    completed = 0
    total = len(new_files)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_and_extract, key, output_dir): key for key in sorted(new_files)}
        for future in as_completed(futures):
            key = futures[future]
            filename = key.split("/")[-1]
            completed += 1
            try:
                result = future.result()
                if result == "ok":
                    success_count += 1
                    print(f"[{completed}/{total}] {filename} ✓")
                elif result == "skip":
                    skip_count += 1
                else:
                    error_count += 1
                    print(f"[{completed}/{total}] {filename} ✗")
            except Exception as e:
                error_count += 1
                print(f"[{completed}/{total}] {filename} ✗ {e}")
    
    print("-" * 50)
    print(f"✓ Downloaded {success_count} new files ({skip_count} skipped, {error_count} errors)")
    
    return success_count


def main():
    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    
    print("=" * 60)
    print(f"  Binance {SYMBOL} USD-M Futures Data Updater")
    print("=" * 60)
    
    total_new = 0
    
    # Process each dataset
    for dataset_name, prefix in DATASETS.items():
        new_count = process_dataset(dataset_name, prefix)
        total_new += new_count
    
    # Summary
    print(f"\n{'='*60}")
    print(f"  Summary: {total_new} total new files downloaded")
    print(f"{'='*60}")
    
    # Run import script if any new data was downloaded
    if total_new > 0:
        print("\nImporting new data into TimescaleDB...")
        print("=" * 50)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        import_script = os.path.join(script_dir, "import_data.py")
        subprocess.run(["python3", import_script])
    else:
        print("\nNo new data to import.")


if __name__ == "__main__":
    main()
