import hopsworks
import pandas as pd
from src.data_utils.ingest import fetch_static_live
from dotenv import load_dotenv
import os
from datetime import date
import io


def load_hopsworks():
    load_dotenv()
    hopsworks_key = os.getenv('HOPSWORKS_API_KEY')
    if hopsworks_key is not None:
        os.environ['HOPSWORKS_API_KEY'] = hopsworks_key

    # If you are invited to someone else's Hopsworks project, write that project's name below
    project_name = None

    if project_name:
        project = hopsworks.login(project=f'{project_name}')
    else:
        project = hopsworks.login()
    fs = project.get_feature_store()

    return project, fs


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


def get_trip_to_line():
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
    fg.delete(f"service_date < '{date.today()}'")


if __name__ == '__main__':
    project, fs = load_hopsworks()
    trip_to_line = get_trip_to_line()
    upload_trip_to_line_mapping(fs, trip_to_line)