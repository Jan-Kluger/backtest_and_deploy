#!/usr/bin/env python3
"""
Download new BTCUSDT 1m kline data from Binance and import into TimescaleDB
"""

import os
import re
import glob
import zipfile
import subprocess
import requests
import xml.etree.ElementTree as ET
from time import sleep
from datetime import datetime

BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
PREFIX = "data/spot/daily/klines/BTCUSDT/1m/"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
DATA_DIR = "./data"
CHUNK = 1024 * 1024


def get_latest_local_date():
    """Get the latest date from existing CSV files"""
    csv_files = glob.glob(os.path.join(DATA_DIR, "BTCUSDT-1m-*.csv"))
    if not csv_files:
        return None
    
    dates = []
    for f in csv_files:
        match = re.search(r'(\d{4}-\d{2}-\d{2})\.csv$', f)
        if match:
            dates.append(datetime.strptime(match.group(1), '%Y-%m-%d').date())
    
    return max(dates) if dates else None


def list_remote_files():
    """List all files from Binance S3"""
    files = []
    marker = ""
    
    while True:
        url = f"{BASE}/?prefix={PREFIX}&delimiter=/"
        if marker:
            url += f"&marker={marker}"
        
        print(f"Listing: {url}")
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


def download_and_extract(key):
    """Download a zip file and extract the CSV"""
    filename = key.split("/")[-1]
    zip_path = os.path.join(DATA_DIR, filename)
    csv_filename = filename.replace(".zip", ".csv")
    csv_path = os.path.join(DATA_DIR, csv_filename)
    url = f"{BASE}/{key}"
    
    # Skip if CSV already exists
    if os.path.exists(csv_path):
        print(f"  [SKIP] {csv_filename} already exists")
        return True
    
    print(f"  [DOWNLOADING] {filename}")
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            if r.status_code != 200:
                print(f"  [ERROR] status {r.status_code}")
                return False
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=CHUNK):
                    if chunk:
                        f.write(chunk)
        
        # Extract CSV from zip
        print(f"  [EXTRACTING] {filename}")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(DATA_DIR)
        
        # Remove zip file
        os.remove(zip_path)
        print(f"  [OK] {csv_filename}")
        return True
        
    except Exception as e:
        print(f"  [FAILED] {filename}: {e}")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return False


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print("=" * 50)
    print("Binance BTCUSDT 1m Data Updater")
    print("=" * 50)
    
    # Get latest local date
    latest_local = get_latest_local_date()
    if latest_local:
        print(f"\n✓ Latest local data: {latest_local}")
    else:
        print("\n✓ No local data found, downloading all")
    
    # List remote files
    print("\nFetching file list from Binance S3...")
    remote_files = list_remote_files()
    print(f"✓ Found {len(remote_files)} files on Binance")
    
    # Filter to only newer files
    if latest_local:
        new_files = []
        for key in remote_files:
            file_date = extract_date_from_key(key)
            if file_date and file_date > latest_local:
                new_files.append(key)
    else:
        new_files = remote_files
    
    if not new_files:
        print("\n✓ Already up to date!")
        return
    
    print(f"\n{len(new_files)} new files to download")
    print("-" * 50)
    
    # Download and extract new files
    success_count = 0
    for i, key in enumerate(sorted(new_files), 1):
        print(f"[{i}/{len(new_files)}] {key.split('/')[-1]}")
        if download_and_extract(key):
            success_count += 1
        sleep(0.05)  # Be nice to S3
    
    print("-" * 50)
    print(f"\n✓ Downloaded {success_count}/{len(new_files)} files")
    
    # Run import script
    if success_count > 0:
        print("\nImporting new data into TimescaleDB...")
        print("=" * 50)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        import_script = os.path.join(script_dir, "import_klines.py")
        subprocess.run(["python3", import_script])


if __name__ == "__main__":
    main()

