from src.data_utils.ingest import fetch_realtime_live, fetch_static_live
from datetime import datetime, date
from zoneinfo import ZoneInfo
import pandas as pd
from collections import defaultdict
import hopsworks
import os
import io
import numpy as np


### This script is run every 30 minutes as a github action
### 1. Fetches realtime VehiclePositions and TripUpdates data
### 2. Creates features and uploads them to hopsworks feature group
### 3. Creates labels and uploads them to hopsworks feature group


def fetch_realtime():
    content_VP = fetch_realtime_live(feed="VehiclePositions")
    content_TU = fetch_realtime_live(feed="TripUpdates")

    return content_VP, content_TU


def get_delay_lags(fs, line, timestamp):
    fg = fs.get_or_create_feature_group(
        name="delay_features_fg",
        description="lagged time features",
        version=1,
        primary_key=["line"],
        event_time="timestamp",
        online_enabled=True
    )
    try: 
        df = fg.read(online=True)
    except Exception:
        df = pd.DataFrame(columns=["timestamp", "line", "delay_60", "delay_30", "delay_current"])
    df = df[
        (df["line"] == line) & (df["timestamp"] < timestamp)
    ].sort_values("timestamp", ascending=False)
    
    return {
        "delay_30": df.iloc[0]["delay_current"] if len(df) > 0 else np.nan,
        "delay_60": df.iloc[1]["delay_current"] if len(df) > 1 else np.nan,
    }


def extract_current_delay_per_line(content_TU, trip_to_line):
    latest_delay_per_trip = {}
    for entity in content_TU.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id
        line = trip_to_line.get(trip_id)
        if line is None: # In this case, the trip is not relevant (non-metro)
            continue
        
        # Now, we need to find the delay at the latest stop
        latest_stu = None
        max_seq = -1
        for stu in trip_update.stop_time_update:
            if stu.stop_sequence > max_seq:
                max_seq = stu.stop_sequence
                latest_stu = stu
        if latest_stu is None: 
            continue

        # Now, we get the actual delay at the latest stop
        delay = None
        if latest_stu.HasField("arrival") and latest_stu.arrival.HasField("delay"):
            delay = latest_stu.arrival.delay
        
        elif latest_stu.HasField("departure") and latest_stu.departure.HasField("delay"):
            delay = latest_stu.departure.delay
        
        if delay is not None:
            latest_delay_per_trip[trip_id] = (line, delay)

    delays_by_line = defaultdict(list)
    for line, delay in latest_delay_per_trip.values():
        delays_by_line[line].append(delay)

    avg_delay_by_line = {}
    for line, delays in delays_by_line.items():
        avg_delay_by_line[line] = sum(delays) / len(delays) if delays else 0.0
    
    return avg_delay_by_line


def compute_and_upload_features(avg_delay, fs):
    now = pd.Timestamp(datetime.now(ZoneInfo("Europe/Stockholm"))).tz_localize(None).floor("min")
    feature_rows = []

    for line, delay_now in avg_delay.items():
        lags = get_delay_lags(fs, line, now) # get lagged delays from hopsworks

        feature_rows.append({
            "timestamp": now,
            "line": line,
            "delay_60": lags["delay_60"],
            "delay_30": lags["delay_30"],
            "delay_current": delay_now
        })
    
    df_features = pd.DataFrame(feature_rows)
    fg = fs.get_or_create_feature_group(
        name="delay_features_fg",
        description="lagged time features",
        version=1,
        primary_key=["line"],
        event_time="timestamp",
        online_enabled=True
    )
    fg.insert(df_features, write_options={"wait_for_job": True})
    # return feature_rows


def compute_and_upload_labels(avg_delay, fs):
    now = pd.Timestamp(datetime.now(ZoneInfo("Europe/Stockholm"))).tz_localize(None).floor("min")
    label_rows = [{"timestamp": now, "line": line, "avg_delay": delay} for line, delay in avg_delay.items()]
    df_labels = pd.DataFrame(label_rows)

    fg = fs.get_or_create_feature_group(
        name="delay_labels_fg",
        description="labels for each line",
        version=1, 
        primary_key=["line"],
        event_time="timestamp",
        online_enabled=True
    )
    fg.insert(df_labels, write_options={"wait_for_job": True})
    # return label_rows


def load_hopsworks():
    hopsworks_key = os.getenv('HOPSWORKS_API_KEY')
    if hopsworks_key:
        os.environ['HOPSWORKS_API_KEY'] = hopsworks_key

    # If you are invited to someone else's Hopsworks project, write that project's name below
    project_name = None

    if project_name:
        project = hopsworks.login(project=f'{project_name}')
    else:
        project = hopsworks.login()
    fs = project.get_feature_store()

    return project, fs


def get_trip_to_line_realtime(fs):
    today = datetime.now(ZoneInfo("Europe/Stockholm")).date().isoformat()
    fg = fs.get_feature_group(
        name="trip_line_mapping_fg"
    )
    try: 
        df = fg.read(online=True)
    except Exception:
        df = None

    if df is None: # no static data
        return None

    latest_date = df["service_date"].max()

    if latest_date != today:
        return None # no static data uploaded for today yet

    df = df[df["service_date"] == latest_date]

    return dict(zip(df.trip_id, df.line))


def check_latest(fs):
    # return true if we should get new data
    today = datetime.now(ZoneInfo("Europe/Stockholm")).date().isoformat()
    fg = fs.get_feature_group(
        name="trip_line_mapping_fg",
        version=1
    )
    try: 
        df = fg.read(online=True)
    except Exception:
        df = None # data does not exist, we should return True so we create data
    
    if df is None or df.empty:
        return True
    
    latest_date = df["service_date"].max()

    if latest_date < today:
        fg.delete()
        return True # no static data uploaded for today yet
    else:
        return False


def get_route_name_mapping(routes):
    relevant = ["Röda linjen", "Blå linjen", "Gröna linjen"]
    df = pd.read_csv(io.StringIO(routes), dtype={'route_id':str})
    filtered_df = df[df['route_long_name'].isin(relevant)]
    return dict(zip(filtered_df['route_id'], filtered_df['route_long_name']))


def get_trip_route_mapping(trips, route_ids):
    mapping = {}
    df = pd.read_csv(io.StringIO(trips), dtype={'trip_id':str, 'route_id':str})
    for _, row in df.iterrows():
        trip_id = row['trip_id']
        route_id = row['route_id']
        if route_id in route_ids and trip_id not in mapping:
            mapping[trip_id] = route_id
    return mapping


def get_trip_to_line_static():
    trip_to_line = {}
    data = fetch_static_live()
    routes = data["routes.txt"]
    trips = data["trips.txt"]
    routes_csv = routes.decode('utf-8')
    trips_csv = trips.decode('utf-8')
    
    route_id_to_name = get_route_name_mapping(routes_csv)
    route_ids = route_id_to_name.keys()
    trip_to_route = get_trip_route_mapping(trips_csv, route_ids)
    for key, value in trip_to_route.items():
        trip_to_line[key] = route_id_to_name[value]
    return trip_to_line


def upload_trip_to_line_mapping(fs, trip_to_line):
    today = date.today().isoformat()
    rows = [
        {
            "trip_id": trip_id,
            "line": line,
            "service_date": today
        }
        for trip_id, line in trip_to_line.items()
    ]
    df = pd.DataFrame(rows)
    fg = fs.get_or_create_feature_group(
        name="trip_line_mapping_fg",
        version=1,
        primary_key=["trip_id"],
        description="Static mapping from GTFS trip_id to metro line",
        online_enabled=True
    )
    fg.insert(df, write_options={"wait_for_job": True})


if __name__ == '__main__':
    print("logging into hopsworks")
    project, fs = load_hopsworks()

    # static handler here
    static_fetched = False
    if check_latest(fs):
        print("Static data not yet uploaded for today. Fetching new data...")
        trip_to_line = get_trip_to_line_static()
        upload_trip_to_line_mapping(fs, trip_to_line)
        static_fetched = True
    
    # realtime handler here
    if static_fetched:
        print("Static data for today has been fetched and uploaded to hopsworks")
    else:
        print("Static data for today already exists. Skipped fetch.")
    
    trip_to_line = get_trip_to_line_realtime(fs)

    if trip_to_line is None:
        print(f"No static data uploaded for today yet, skipping ingestion.")
        exit(0)

    content_VP, content_TU = fetch_realtime()
    avg_delay_by_line = extract_current_delay_per_line(content_TU, trip_to_line)
    compute_and_upload_features(avg_delay_by_line, fs)
    compute_and_upload_labels(avg_delay_by_line, fs)

    # print("Outputting results...")
    # print("trip_to_line!")
    # print(f"{trip_to_line.items()}\n")

    # print(f"printing avg_delay_by_line...")
    # print(f"{avg_delay_by_line.items()}")

    # print(f"printing label rows...")
    # for i, d in enumerate(label_rows):
    #     print(f'printing element {i+1} in label rows')
    #     print(f'{d.items()}\n')
