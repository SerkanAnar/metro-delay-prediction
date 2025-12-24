import pandas as pd
import os
import json
from pathlib import Path
from tqdm import tqdm
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict


def filter_routes(original_path, target_path):
    """
        Filters out non-metro routes from routes.csv and creates a filtered csv file

        :param original_path:   Path to the original routes.csv file
        :param target_path:     Path where the filtered csv should be saved, including filename
        :return:                Set of relevant route ids
    """

    if not os.path.exists(target_path):
        relevant_line_names = ['Blå linjen', 'Röda linjen', 'Gröna linjen']
        df = pd.read_csv(original_path, dtype={'route_id':str})
        filtered_df = df[df['route_long_name'].isin(relevant_line_names)]
        filtered_df.to_csv(target_path, index=False)

    df = pd.read_csv(target_path, dtype={'route_id':str})
    relevant_route_ids = set(df['route_id'])
    return relevant_route_ids


def filter_trips(original_path, target_path, relevant_route_ids):
    """
        Filters out non-relevant trip ids in trips.csv and creates a filtered csv file

        :param original_path:       Path to the original trips.csv file
        :param target_path:         Path where the filtered csv should be saved, including filename
        :param relevant_route_ids:  Set of relevant route ids
        :return:                    Set of relevant shape ids
    """

    if not os.path.exists(target_path):
        df = pd.read_csv(original_path, dtype={'route_id':str})
        filtered_df = df[df['route_id'].isin(relevant_route_ids)]
        filtered_df.to_csv(target_path, index=False)
    
    df = pd.read_csv(target_path, dtype={'route_id':str, 'shape_id':str})
    relevant_shape_ids = set(df['shape_id'])
    return relevant_shape_ids


def filter_shapes(original_path, target_path, relevant_shape_ids):
    """
        Filters out non-relevant shape ids in shapes.csv and creates a filtered csv file

        :param original_path:       Path to the original shapes.csv file
        :param target_path:         Path where the filtered csv should be saved, including filename
        :param relevant_shape_ids:  Set of relevant shape ids
    """

    if not os.path.exists(target_path):
        df = pd.read_csv(original_path, dtype={'shape_id':str})
        filtered_df = df[df['shape_id'].isin(relevant_shape_ids)]
        filtered_df.to_csv(target_path, index=False)


def filter_stop_times(original_path, target_path, relevant_trip_ids):
    """
        Filters out non-relevant trip ids in stop_times.csv and creates a filtered csv file

        :param original_path:       Path to the original stop_times.csv file
        :param target_path:         Path where the filtered csv should be saved, including filename
        :param relevant_trip_ids:   Set of relevant trip ids
        :return:                    Set of relevant stop ids
    """

    if not os.path.exists(target_path):
        df = pd.read_csv(original_path, dtype={'trip_id':str})
        filtered_df = df[df['trip_id'].isin(relevant_trip_ids)]
        filtered_df.to_csv(target_path, index=False)
    
    df = pd.read_csv(target_path, dtype={'trip_id':str, 'stop_id':str})
    relevant_stop_ids = set(df['stop_id'])
    return relevant_stop_ids


def filter_stops(original_path, target_path, relevant_stop_ids):
    """
        Filters out non-relevant stop ids in stops.csv and creates a filtered csv file

        :param original_path:       Path to the original stops.csv file
        :param target_path:         Path where the filtered csv should be saved, including filename
        :param relevant_stop_ids:   Set of relevant stop ids
    """

    if not os.path.exists(target_path):
        df = pd.read_csv(original_path, dtype={'stop_id':str})
        filtered_df = df[df['stop_id'].isin(relevant_stop_ids)]
        filtered_df.to_csv(target_path, index=False)


def get_trip_ids(path):
    """
        Finds relevant trip ids in the filtered trips.csv file

        :param path:    Path to the filtered trips.csv file
        :return:        Set of relevant trip ids
    """

    df = pd.read_csv(path, dtype={'trip_id':str})
    relevant_trip_ids = set(df['trip_id'])
    return relevant_trip_ids


def compare_files(original_path, filtered_path):
    """
        Prints information of the original and filtered csv file for comparative purposes

        :param original_path:   Path to the original csv file
        :param filtered_path:   Path to the filtered csv file
    """

    if os.path.exists(original_path) and os.path.exists(filtered_path):
        df_original = pd.read_csv(original_path)
        df_filtered = pd.read_csv(filtered_path)
        print(f'Printing information of {original_path[original_path.index('/')+1:]}')
        print(df_original.info())
        print(f'Printing information of {filtered_path[filtered_path.index('/')+1:]}')
        print(df_filtered.info())


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


def filter_TU_snapshot(snapshot, relevant_trip_ids):
    """
        Filters TripUpdates feed by trip ids

        :param snapshot:            Snapshot of TripUpdates at a given second
        :param relevant_trip_ids:   Set of relevant trip ids
        :return:                    Filtered snapshot
    """

    relevant_entities = []
    for entity in snapshot.get('entity', []):
        trip = entity.get('trip_update', {}).get('trip', {}).get('trip_id')
        if trip in relevant_trip_ids:
            relevant_entities.append(entity)
    snapshot['entity'] = relevant_entities
    return snapshot


def filter_VP_snapshot(snapshot, relevant_trip_ids):
    """
        Filters VehiclePositions feed by trip ids

        :param snapshot:            Snapshot of VehiclePositions at a given second
        :param relevant_trip_ids:   Set of relevant trip ids
        :return:                    Filtered snapshot
    """

    relevant_entities = []
    for entity in snapshot.get('entity', []):
        trip = entity.get('vehicle', {}).get('trip', {}).get('trip_id')
        if trip in relevant_trip_ids:
            relevant_entities.append(entity)
    snapshot['entity'] = relevant_entities
    return snapshot


def preprocess_and_aggregate_VP(DATA_ROOT, date, relevant_trip_ids):
    """
        Filters and aggregates VehiclePosition data into a single file

        :param DATA_ROOT:           Data directory containing the GTFS data folders /static and /realtime
        :param date:                The date of the dataset to process
        :param relevant_trip_ids:   Set of relevant trip ids
    """

    raw_RT_dir = DATA_ROOT / 'realtime' / date / 'VehiclePositions' / 'raw'
    output_dir = DATA_ROOT / 'realtime' / date / 'VehiclePositions' / 'hourly'
    output_dir.mkdir(parents=True, exist_ok=True)
    for folder in raw_RT_dir.iterdir():
        if not folder.is_dir(): continue
        for f in folder.iterdir():
            if not f.suffix == '.pb': continue
            path_to_file = folder / f.stem
            pb_to_json(path_to_file)

        hour = folder.name
        hourly_snapshots = []
        if os.path.exists(output_dir / hour): continue

        print('Filtering RT json files')
        json_files = list(folder.glob("*.json"))
        for json_file in tqdm(json_files, desc=f"Filtering snapshots for hour {hour}"):
            with open(json_file, 'r', encoding='utf-8') as f:
                snapshot = json.load(f)
            snapshot = filter_VP_snapshot(snapshot, relevant_trip_ids)
            if snapshot.get('entity'):
                hourly_snapshots.append({
                    'timestamp': snapshot['header']['timestamp'],
                    'entity': snapshot['entity']
                })
        print('Done filtering RT json files')
        print(f'Writing snapshots to {hour}.json')
        with open(output_dir / f'{hour}.json', 'w', encoding='utf-8') as f:
            json.dump({
                'date': date,
                'hour': hour,
                'snapshots': hourly_snapshots
            }, f, indent=2)
        print(f'Preprocessed, filtered and aggregated hour {hour} with {len(hourly_snapshots)} snapshots')


def preprocess_and_aggregate_TU(DATA_ROOT, date, relevant_trip_ids):
    """
        Filters and aggregates TripUpdates data into a single file

        :param DATA_ROOT:           Data directory containing the GTFS data folders /static and /realtime
        :param date:                The date of the dataset to process
        :param relevant_trip_ids:   Set of relevant trip ids
    """

    raw_TU_dir = DATA_ROOT / 'realtime' / date / 'TripUpdates' / 'raw'
    output_dir = DATA_ROOT / 'realtime' / date / 'TripUpdates' / 'hourly'
    output_dir.mkdir(parents=True, exist_ok=True)
    for folder in raw_TU_dir.iterdir():
        if not folder.is_dir(): continue
        for f in folder.iterdir():
            if not f.suffix == '.pb': continue
            path_to_file = folder / f.stem
            pb_to_json(path_to_file)
        
        hour = folder.name
        hourly_snapshots = []
        if os.path.exists(output_dir / hour): continue

        print('Filtering TU json files')
        json_files = list(folder.glob("*.json"))
        for json_file in tqdm(json_files, desc=f'Filtering snapshots for hour {hour}'):
            with open(json_file, 'r', encoding='utf-8') as f:
                snapshot = json.load(f)
            snapshot = filter_TU_snapshot(snapshot, relevant_trip_ids)
            if snapshot.get('entity'):
                hourly_snapshots.append({
                    'timestamp': snapshot['header']['timestamp'],
                    'entity': snapshot['entity']
                })
        print('Done filtering TU json files')
        print(f'Writing snapshots to {hour}.json')
        with open(output_dir / f'{hour}.json', 'w', encoding='utf-8') as f:
            json.dump({
                'date': date,
                'hour': hour,
                'snapshots': hourly_snapshots
            }, f, indent=2)
        print(f'Preprocessed, filtered and aggregated TU hour {hour} with {len(hourly_snapshots)} snapshots')


def filter_static(original_paths, filtered_dir):
    """
        Runs the full static data filtering pipeline

        :param original_paths:  Dictionary mapping filenames to original csv paths
        :param filtered_dir:    Path to the directory where the filtered csv files are saved
        :return:                Set of relevant trip ids
    """

    routes_output_dir = filtered_dir / "routes.csv"
    relevant_route_ids = filter_routes(original_paths['routes'], routes_output_dir)

    trips_output_dir = filtered_dir / "trips.csv"
    relevant_shape_ids = filter_trips(original_paths['trips'], trips_output_dir, relevant_route_ids)
    
    relevant_trip_ids = get_trip_ids(trips_output_dir)

    shapes_output_dir = filtered_dir / "shapes.csv"
    filter_shapes(original_paths['shapes'], shapes_output_dir, relevant_shape_ids)

    stop_times_output_dir = filtered_dir / "stop_times.csv"
    relevant_stop_ids = filter_stop_times(original_paths['stop_times'], stop_times_output_dir, relevant_trip_ids)

    stops_output_dir = filtered_dir / "stops.csv"
    filter_stops(original_paths['stops'], stops_output_dir, relevant_stop_ids)
    return relevant_trip_ids


def filter_data_for_date(DATA_ROOT, date):
    """
        Filters GTFS data for a specific date

        :param DATA_ROOT:   Data directory containing the GTFS data folders /static and /realtime
        :param date:        The date of the dataset to process
    """

    # Handle static and RT data
    static_directory = DATA_ROOT / "static" / date
    static_original_paths = {f.stem: f for f in static_directory.glob("*.csv")}
    filtered_dir = static_directory.with_name(static_directory.name + "-filtered")
    filtered_dir.mkdir(exist_ok=True)

    relevant_trip_ids = filter_static(static_original_paths, filtered_dir)
    preprocess_and_aggregate_VP(DATA_ROOT, date, relevant_trip_ids)
    preprocess_and_aggregate_TU(DATA_ROOT, date, relevant_trip_ids)


def filter_irrelevant_files(DATA_ROOT, date):
    """
        Filters out irrelevant files in the specified data directory
            :param1 DATA_ROOT:  Data directory containing the GTFS data folders /static and /realtime
            :param2 date:       The date of the dataset to process
    """

    output_dir = DATA_ROOT / "static" / date
    output_dir.mkdir(parents=True, exist_ok=True)
    keep = {"routes.csv", "shapes.csv", "stop_times.csv", "stops.csv", "trips.csv"}
    for path in output_dir.iterdir():
        if path.is_file() and path.name not in keep:
            path.unlink()
    print(f"Removed unnecessary files in {output_dir}")


if __name__ == '__main__':
    DATA_ROOT = Path('data')
    date = '2025-12-12'
    filter_irrelevant_files(DATA_ROOT, date)
    filter_data_for_date(DATA_ROOT, date)