import pandas as pd
from collections import defaultdict
import json

def get_trip_route_mapping(target_path):
    mapping = {}
    df = pd.read_csv(target_path, dtype={'route_id': str, 'trip_id': str})
    for _, row in df.iterrows():
        trip_id = row['trip_id']
        route_id = row['route_id']
        if trip_id not in mapping:
            mapping[trip_id] = route_id
    return mapping

def get_route_name_mapping(target_path):
    mapping = {}
    df = pd.read_csv(target_path, dtype={'route_id': str})
    for _, row in df.iterrows():
        route_name = row['route_long_name']
        route_id = row['route_id']
        mapping[route_id] = route_name
    return mapping

def get_trip_to_line_mapping():
    trip_to_line = {}
    route_id_to_name = get_route_name_mapping('data/static/2025-12-23/routes.csv')
    trip_to_route_id = get_trip_route_mapping('data/static/2025-12-23/trips.csv')
    for key, value in trip_to_route_id.items():
        trip_to_line[key] = route_id_to_name[value]
    return trip_to_line

def init_stats():
    return {
        "vehicle_count": 0,
        "speed_sum": 0.0,
        "stopped_count": 0,
        "unique_trips": set()
    }

def get_features(target_path, trip_to_line):
    stats = {
        "green": init_stats(),
        "red": init_stats(),
        "blue": init_stats()
    }
    
    with open(target_path, 'r', encoding="utf-8") as f:
        data = json.load(f)

    for snapshot in data["snapshots"]:
        for entity in snapshot["entity"]:
            vehicle = entity.get("vehicle", {})
            trip_id = vehicle["trip"]["trip_id"]
            speed = vehicle["position"]["speed"]

            line_name = trip_to_line[trip_id]
            stats[line_name]["vehicle_count"] += 1
            stats[line_name]["speed_sum"] += speed
            stats[line_name]["unique_trips"].add(trip_id)

            if speed <= 0.5:
                stats[line_name]["stopped_count"] += 1
    
    features = []
    for line, s in stats.items():
        features.append({
            "date": "2025-12-12",
            "hour": 10,
            "line": line,
            "avg_speed": s["speed_sum"] / s["vehicle_count"],
            "num_active_trips": s["unique_trips"],
            "frac_stopped": s["stopped_count"] / s["vehicle_count"]
        })
    
    return features

def get_labels(target_path, trip_to_line):
    delays_by_line = defaultdict(list)
    with open(target_path, 'r', encoding='utf-8') as f:
        trip_updates_hourly = json.load(f)
    
    for snapshot in trip_updates_hourly["snapshots"]:
        for entity in snapshot.get("entity", []):
            trip_update = entity.get("trip_update", {})
            trip = trip_update.get("trip", {})
            trip_id = trip.get("trip_id")

            if trip_id not in trip_to_line: 
                continue

            line = trip_to_line[trip_id]

            for stu in trip_update.get("stop_time_update", []):
                arrival = stu.get("arrival", {})

                if not arrival: 
                    continue

                delay = arrival.get("delay")

                if delay is None: 
                    continue

                delays_by_line[line].append(delay)
    
    avg_delay_by_line = {}
    for line, delays in delays_by_line.items():
        if delays:
            avg_delay_by_line[line] = sum(delays) / len(delays)
        else:
            avg_delay_by_line[line] = None
    return avg_delay_by_line

if __name__ == '__main__':
    trip_to_line = get_trip_to_line_mapping() # This allows us to go from trip id -> route name
    path_to_file = 'data/realtime/2025-12-12/TripUpdates/hourly/10.json'
    avg_delay_by_line = get_labels(path_to_file, trip_to_line) # Here are our labels for hour 2025-12-12, hour 10
    path_to_realtime = 'data/realtime/2025-12-12/VehiclePositions/hourly/10.json'
    features = get_features(path_to_realtime, trip_to_line)
    print(avg_delay_by_line)
    print(features)
