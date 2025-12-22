from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import json
import requests
import os
from pathlib import Path
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
        
    print(f"Saved GTFS static file to {file_path}.")
    return file_path


def zip_to_txt(target_dir):
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


def fetch_realtime(date, target_dir, feed="VehiclePositions", hour=None, wait_seconds=30):
    """
        Fetches real time public transport data using Trafiklab's KoDa API
        
        :param date:         Specifies which date the data is fetched from, in YYYY-MM-DD format
        :param target_dir:   Directory that the API output should be saved at, without file name
        :param feed:         Specifies which feed [ServiceAlerts, TripUpdates, VehiclePositions] to fetch from
        :param hour:         Use in [00-23] format to specify hour; fetches entire day if empty
        :param wait_seconds: Number of seconds the function waits before retrying API call
        :return:             Path of the saved .zip folder
    """
    load_dotenv()
    api_key = os.getenv("KODA_API_KEY")
    
    if hour is None:
        url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/sl/{feed}?date={date}&key={api_key}'
    else:
        url = f'https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/sl/{feed}?date={date}&hour={hour}&key={api_key}'
    
    while True:
        try:
            response = requests.get(url, timeout=(5,10))
        
        except requests.exceptions.ReadTimeout:
            print(f"Retrying connection in {wait_seconds} seconds")
            time.sleep(wait_seconds)
            continue
        
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "").lower()

        if "application/json" in content_type:
            data = response.json()
            if "message" in data and "being processed" in data["message"]:
                print(f"Still processing, retrying in {wait_seconds} seconds")
                time.sleep(wait_seconds)
                continue

        break
    
    file_path = os.path.join(target_dir, f'{date}.7z')
    
    with open(file_path, 'wb') as f:
        f.write(response.content)
        
    print(f"Saved GTFS realtime file to {file_path}.")
    return file_path


def pb_to_json(target_dir):
    """
        Converts .pb files into .json
        
        :param target_dir: Directory the .pb file is in, with the file name, excluding ".pb"
        :return:           Path of the saved .json file
    """

    feed = gtfs_realtime_pb2.FeedMessage()

    with open(f'{target_dir}.pb', 'rb') as f:
        feed.ParseFromString(f.read())

    feed_dict = MessageToDict(
        feed,
        preserving_proto_field_name=True
    )

    with open(f'{target_dir}.json', 'w', encoding='utf-8') as f:
        json.dump(feed_dict, f, indent=2)
    
    pb_path = Path(f'{target_dir}.pb')
    pb_path.unlink()
        
    print(f"Converted {target_dir}.pb to {target_dir}.json.")
    return f'{target_dir}.json'


# TESTING

# zip_file = fetch_static("2025-12-12", "data")
# zip_dir = zip_to_txt(zip_file)
# txt_to_csv(zip_dir)
fetch_realtime("2025-12-17", "data", hour=10)
# pb_to_json("data/2025-12-12")
