from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import json
import requests
import os


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


def pb_to_json(target_dir):
    """
        Converts .pb files into .json.
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
        
    print(f"Converted {target_dir}.pb to {target_dir}.json.")
    return f'{target_dir}.json'


# TESTING

pb_to_json('data/realtime/realtime')
# fetch_static("2025-12-12", "data")
# pb_to_json("data/test")