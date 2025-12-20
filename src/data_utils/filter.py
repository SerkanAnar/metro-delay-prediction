import pandas as pd
import os

def clean_routes(original_path, target_path):
    if not os.path.exists(target_path):
        relevant_line_names = ['Blå linjen', 'Röda linjen', 'Gröna linjen']
        df = pd.read_csv(original_path, dtype={'route_id':str})
        filtered_df = df[df['route_long_name'].isin(relevant_line_names)]
        filtered_df.to_csv(target_path, index=False)

    df = pd.read_csv(target_path, dtype={'route_id':str})
    relevant_route_ids = set(df['route_id'])
    return relevant_route_ids

def clean_trips(original_path, target_path, relevant_route_ids):
    if not os.path.exists(target_path):
        df = pd.read_csv(original_path)
        filtered_df = df[df['route_id'].isin(relevant_route_ids)]
        filtered_df.to_csv(target_path, index=False)
    
    df = pd.read_csv(target_path, dtype={'route_id':str, 'shape_id':str})
    relevant_shape_ids = set(df['shape_id'])
    return relevant_shape_ids
    
def clean_shapes(original_path, target_path, relevant_shape_ids):
    if not os.path.exists(target_path):
        df = pd.read_csv(original_path, dtype={'shape_id':str})
        filtered_df = df[df['shape_id'].isin(relevant_shape_ids)]
        filtered_df.to_csv(target_path, index=False)

def clean_stop_times(original_path, target_path, relevant_trip_ids):
    if not os.path.exists(target_path):
        df = pd.read_csv(original_path, dtype={'trip_id':str})
        filtered_df = df[df['trip_id'].isin(relevant_trip_ids)]
        filtered_df.to_csv(target_path, index=False)
    
    df = pd.read_csv(target_path, dtype={'trip_id':str, 'stop_id':str})
    relevant_stop_ids = set(df['stop_id'])
    return relevant_stop_ids

def clean_stops(original_path, target_path, relevant_stop_ids):
    if not os.path.exists(target_path):
        df = pd.read_csv(original_path, dtype={'stop_id':str})
        filtered_df = df[df['stop_id'].isin(relevant_stop_ids)]
        filtered_df.to_csv(target_path, index=False)

def get_trip_ids(path):
    """
    Finds all relevant trip ids.
    :path is the path to the 
    :return: returns relevant trip ids
    """ 
    df = pd.read_csv(path, dtype={'trip_id':str})
    relevant_trip_ids = set(df['trip_id'])
    return relevant_trip_ids

def compare_files(original, filtered):
    if os.path.exists(original) and os.path.exists(filtered):
        df_original = pd.read_csv(original)
        df_filtered = pd.read_csv(filtered)
        print(f'Printing information of {original[original.index('/')+1:]}')
        print(df_original.info())
        print(f'Printing information of {filtered[filtered.index('/')+1:]}')
        print(df_filtered.info())

def filter_and_save(all_paths):
    relevant_route_ids = clean_routes(all_paths['routes'][0], all_paths['routes'][1])
    relevant_shape_ids = clean_trips(all_paths['trips'][0], all_paths['trips'][1], relevant_route_ids)
    relevant_trip_ids = get_trip_ids(all_paths['trips'][1])
    clean_shapes(all_paths['shapes'][0], all_paths['shapes'][1], relevant_shape_ids)
    relevant_stop_ids = clean_stop_times(all_paths['stop_times'][0], all_paths['stop_times'][1], relevant_trip_ids)
    clean_stops(all_paths['stops'][0], all_paths['stops'][1], relevant_stop_ids)

if __name__ == '__main__':
    all_paths = {'routes': ['GTFS-SL-2025-12-10/routes.csv', 'GTFS-SL-2025-12-10-filtered/routes_filtered.csv'],
                 'trips': ['GTFS-SL-2025-12-10/trips.csv', 'GTFS-SL-2025-12-10-filtered/trips_filtered.csv'],
                 'shapes': ['GTFS-SL-2025-12-10/shapes.csv', 'GTFS-SL-2025-12-10-filtered/shapes_filtered.csv'],
                 'stop_times': ['GTFS-SL-2025-12-10/stop_times.csv', 'GTFS-SL-2025-12-10-filtered/stop_times_filtered.csv'],
                 'stops': ['GTFS-SL-2025-12-10/stops.csv', 'GTFS-SL-2025-12-10-filtered/stops_filtered.csv']}

    filter_and_save(all_paths)
    # compare_files(all_paths['stops'][0], all_paths['stops'][1])