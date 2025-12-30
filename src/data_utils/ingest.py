from datetime import datetime
from google.transit import gtfs_realtime_pb2
from io import BytesIO
import requests
import os
import time
import zipfile


def fetch_static_live(file_names=['routes.txt', 'trips.txt']):
    """
    Fetches static planned public transport data at inference from today using Trafiklab's GTFS Regional Static Data API
    
    :param file_names: List of file names from the fetched .zip file to output
    :return:           The content of the specified files in the fetched .zip file in dictionary format
    """
    
    # load_dotenv()
    api_key = os.getenv("STATIC_API_KEY")
    
    url = f'https://opendata.samtrafiken.se/gtfs/sl/sl.zip?key={api_key}'
    
    date = datetime.today().strftime('%Y-%m-%d')
    
    for attempt in range(5):
        try:
            response = requests.get(url)
            response.raise_for_status()
        except:
            print(f'HTTP status {response.status_code}: {response.text}')
            print(f'Retrying in 5 seconds')
            time.sleep(5)
            continue
        break
    else:
        print(f'Skipping static data fetch for {date}')
        return None
        
    data = {}
    with zipfile.ZipFile(BytesIO(response.content)) as z:
        for name in file_names:
            if name in z.namelist():
                with z.open(name) as f:
                    data[name] = f.read()
    return data


def fetch_realtime_live(feed="VehiclePositions", wait_seconds=5, max_retries=5):
    """
    Fetches real time public transport data using Trafiklab's GTFS Regional Realtime Data API
    
    :param feed:         Specifies which feed [ServiceAlerts, TripUpdates, VehiclePositions] to fetch from
    :param wait_seconds: Number of seconds the function waits before retrying API call
    :param max_retries:  Number of maximum retries
    :return:             Either the realtime contents in .pb format or None if fetch failed
    """
    
    # load_dotenv()
    api_key = os.getenv("REALTIME_API_KEY")
    
    url = f'https://opendata.samtrafiken.se/gtfs-rt/sl/{feed}.pb?key={api_key}'
    
    date = datetime.today().strftime('%Y-%m-%d')
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
        except:
            print(f'HTTP status {response.status_code}: {response.text}')
            print(f'Retrying in {wait_seconds} seconds')
            time.sleep(5)
            continue
        break
    else:
        print(f'Skipping realtime data fetch for {date}')
        return None
    
    data = gtfs_realtime_pb2.FeedMessage()
    data.ParseFromString(response.content)
    return data


# TESTING
if __name__ == '__main__':
    output = fetch_static_live()
    # print(output['routes.txt'])
    fetch_realtime_live()