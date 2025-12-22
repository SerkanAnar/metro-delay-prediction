import pandas as pd
import os
import json
from pathlib import Path

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
        :returns:       Set of relevant trip ids
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


def filter_realtime_data(original_path, target_path, relevant_trip_ids):
    """
        Filters out non-relevant entities from the realtime (JSON) data

        :param original_path:       Path to the realtime .json file
        :param target_path:         Path where the filtered .json should be saved, including filename
        :param relevant_trip_ids:   Set of relevant trip ids
    """
    if os.path.exists(target_path):
        return

    with open(original_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    relevant_entities = []
    for entity in data.get('entity', []):
        trip = entity.get('vehicle', {}).get('trip', {}).get('trip_id')
        if trip in relevant_trip_ids:
            relevant_entities.append(entity)
    
    data['entity'] = relevant_entities
    with open(target_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def filter_static(original_paths, filtered_dir):
    """
        Runs the full static data filtering pipeline

        :param original_paths:  Dictionary mapping filenames to original csv paths
        :param filtered_dir:    Path to the directory where the filtered csv files are saved
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


def filter_data_for_date(DATA_ROOT, date):
    """
        Filters GTFS data for a specific date

        :param DATA_ROOT:   Data directory containing the GTFS data folders /static and /realtime
        :param date:        The date of the dataset to process
    """

    # handle static data first
    static_directory = DATA_ROOT / "static" / date
    static_original_paths = {f.stem: f for f in static_directory.glob("*.csv")}
    filtered_dir = static_directory.with_name(static_directory.name + "-filtered")
    filtered_dir.mkdir(exist_ok=True)

    filter_static(static_original_paths, filtered_dir)

    # TODO: handle realtime data after determining folder structure
    pass


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