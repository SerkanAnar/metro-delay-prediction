import pandas as pd
import os
import json

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
        df = pd.read_csv(original_path)
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


def filter_data(all_paths):
    """
        Handles the filtering pipeline for testing purposes
        :param all_paths:   Dictionary containing all of the relevant paths
    """
    relevant_route_ids = filter_routes(all_paths['routes'][0], all_paths['routes'][1])
    relevant_shape_ids = filter_trips(all_paths['trips'][0], all_paths['trips'][1], relevant_route_ids)
    relevant_trip_ids = get_trip_ids(all_paths['trips'][1])
    filter_shapes(all_paths['shapes'][0], all_paths['shapes'][1], relevant_shape_ids)
    relevant_stop_ids = filter_stop_times(all_paths['stop_times'][0], all_paths['stop_times'][1], relevant_trip_ids)
    filter_stops(all_paths['stops'][0], all_paths['stops'][1], relevant_stop_ids)

    realtime_paths = all_paths['realtime']
    filter_realtime_data(realtime_paths[0], realtime_paths[1], relevant_trip_ids)


if __name__ == '__main__':
    all_paths = {'routes': ['data/static/2025-12-10/routes.csv', 'data/static/2025-12-10-filtered/routes.csv'],
                 'trips': ['data/static/2025-12-10/trips.csv', 'data/static/2025-12-10-filtered/trips.csv'],
                 'shapes': ['data/static/2025-12-10/shapes.csv', 'data/static/2025-12-10-filtered/shapes.csv'],
                 'stop_times': ['data/static/2025-12-10/stop_times.csv', 'data/static/2025-12-10-filtered/stop_times.csv'],
                 'stops': ['data/static/2025-12-10/stops.csv', 'data/static/2025-12-10-filtered/stops.csv'],
                 'realtime': ['data/realtime/realtime.json', 'data/realtime/realtime-filtered.json']}

    filter_data(all_paths)
    # compare_files(all_paths['stops'][0], all_paths['stops'][1])