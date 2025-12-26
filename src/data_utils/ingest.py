from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import json
import requests
import shutil
import os
from pathlib import Path
import py7zr
import time
import zipfile


def fetch_static(date, target_dir):
    """
        Fetches static planned public transport data using Trafiklab's KoDa API
        
        :param date:       Specifies which date the data is fetched from, in YYYY-MM-DD format
        :param target_dir: Directory that the API output should be saved at, without file name
        :return:           Path of the saved .zip folder
    """
    
    load_dotenv()
    api_key = os.getenv("KODA_API_KEY")
    
    url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-static/sl?date={date}&key={api_key}'
    
    response = requests.get(url)
    response.raise_for_status()
    
    file_path = os.path.join(target_dir, f'{date}.zip')
    
    with open(file_path, 'wb') as f:
        f.write(response.content)
        
    print(f"Saved GTFS static file to {file_path}")
    return file_path


def extract_zip(target_dir):
    """
    Extracts .zip files into .txt files and removes the .zip file
    
    :param target_dir: Target .zip file to extract
    :return:           Path to the folder with the extracted files
    """

    zip_path = Path(target_dir)
    if zip_path.suffix != ".zip":
        raise ValueError("Expected a .zip file")
    base_name = zip_path.stem
    base_dir = zip_path.parent
    
    output_dir = os.path.join(base_dir, 'static')
    output_dir = Path(os.path.join(output_dir, base_name))
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(output_dir)
    print(f"Successfully extracted {zip_path} to {output_dir}")
        
    zip_path.unlink()
    print(f"Removed {zip_path}")

    return output_dir


def txt_to_csv(target_dir):
    """
    Converts .txt files in a directory to .csv files
    
    :param target_dir: Directory where all .txt files will be converted to .csv files
    :return:           Path to the directory
    """

    directory = Path(target_dir)
    for path in directory.glob("*.txt"):
        path.rename(path.with_suffix(".csv"))
    return directory


def fetch_realtime(date, target_dir, feed="VehiclePositions", hour=None, wait_seconds=60, max_retries=65):
    """
        Fetches real time public transport data using Trafiklab's KoDa API
        
        :param date:         Specifies which date the data is fetched from, in YYYY-MM-DD format
        :param target_dir:   Directory that the API output should be saved at, without file name
        :param feed:         Specifies which feed [ServiceAlerts, TripUpdates, VehiclePositions] to fetch from
        :param hour:         Use in [0-23] format to specify hour; fetches entire day if empty
        :param wait_seconds: Number of seconds the function waits before retrying API call
        :param max_retries:  Number of maximum retries
        :return:             Path of the saved .zip folder
    """
    load_dotenv()
    api_key = os.getenv("KODA_API_KEY")
    
    if hour is None:
        url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/sl/{feed}?date={date}&key={api_key}'
    else:
        url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/sl/{feed}?date={date}&hour={hour}&key={api_key}'
    
    for attempt in range(1, max_retries+1):
        try:
            response = requests.get(url, timeout=10)
        
        except Exception as e:
            print(f"[{attempt}/{max_retries}] Request failed ({e}), retrying connection in {wait_seconds} seconds")
            time.sleep(wait_seconds)
            continue
        
        content_type = response.headers.get("Content-Type", "").lower()
        if "application/json" in content_type:
            data = response.json()
            if "message" in data and "being prepared" in data["message"]:
                print(f"[{attempt}/{max_retries}] Data is being prepared, retrying connection in {wait_seconds} seconds")
                time.sleep(wait_seconds)
                continue
            if "message" in data and "being processed" in data["message"]:
                print(f"[{attempt}/{max_retries}] Still processing, retrying connection in {wait_seconds} seconds")
                time.sleep(wait_seconds)
                continue
            
        break
    else:
        print(f"Max retries reached, skipping date {date}")
        return None
    
    file_path = os.path.join(target_dir, f'{date}.7z')
    
    with open(file_path, 'wb') as f:
        f.write(response.content)
        
    print(f"Saved GTFS realtime file to {file_path}.")
    return file_path


def extract_7z(target_dir, feed="VehiclePositions", hour=None):
    """
    Extracts .7z files into its own folder in the target directory
    
    :param target_dir: Target .7z file to extract
    :param feed:       Specifies extraction folder [ServiceAlerts, TripUpdates, VehiclePositions]
    :param hour:       Specify to extract the files into an hour subdirectory
    :return:           Path to the folder with the extracted files
    """

    zip_path = Path(target_dir)
    base_name = zip_path.stem
    base_dir = zip_path.parent
    
    output_dir = os.path.join(base_dir, 'realtime')
    output_dir = os.path.join(output_dir, base_name)
    output_dir = os.path.join(output_dir, feed)
    output_dir = Path(os.path.join(output_dir, 'raw'))
    if hour is not None:
        output_dir = Path(os.path.join(output_dir, str(hour)))
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with py7zr.SevenZipFile(zip_path, mode="r") as z:
        z.extractall(path=output_dir)
    print(f"Successfully extracted {zip_path} to {output_dir}")
        
    zip_path.unlink()
    print(f"Removed {zip_path}")

    return output_dir


def flatten_extracted_structure(target_dir, hourly=False):
    """
    Flattens the file structure of the extracted realtime data
    
    :param target_dir: Directory of the extracted 7z files
    :param hourly:     Optional, add to specify if only an hour should be flattened
    :return:           Path of the directory with all .pb files
    """
    
    raw_dir = Path(target_dir)
    
    if hourly:
        hourly_dir = list(raw_dir.glob("sl/*/*/*/*/*"))
    else:
        hourly_dir = list(raw_dir.glob("sl/*/*/*/*"))
        
    for hour_dir in hourly_dir:
        for item in hour_dir.iterdir():
            shutil.move(str(item), raw_dir / item.name)
            
    shutil.rmtree(raw_dir / "sl")
    print(f"Successfully flattened {raw_dir}")
    return raw_dir


# TESTING
if __name__ == '__main__':
    # zip_file = fetch_static("2025-12-12", "data")
    # zip_dir = extract_zip(zip_file)
    # txt_to_csv(zip_dir)
    realtime_file = fetch_realtime("2025-12-12", "data", feed='TripUpdates', hour=10)
    extracted = extract_7z(realtime_file, feed='TripUpdates', hour=10)
    flatten_extracted_structure(extracted)
    # pb_to_json("data/2025-12-12")