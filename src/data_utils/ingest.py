from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
import requests
import os


def fetch_static(date, target_dir):
    """
        Fetches static planned public transport data using Trafiklab's KoDa API.
        :date specifies which date the data is fetched from, in YYYY-MM-DD format.
        :target_dir is the directory that the API output should be saved at, without file name.
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


def pb_to_json(target_dir):
    """
        Converts .pb files into .json.
        :target_dir is the directory the .pb file is in, with the file name, excluding ".pb".
    """
    feed = gtfs_realtime_pb2.FeedMessage()

    with open(f'{target_dir}.pb', 'rb') as f:
        feed.ParseFromString(f.read())

    with open(f'{target_dir}.json', 'w', encoding='utf-8') as f:
        f.write(str(feed))
        
    print(f"Converted {target_dir}.pb to {target_dir}.json.")
    

# TESTING

fetch_static("2025-12-12", "data")
# pb_to_json("data/test")