from google.transit import gtfs_realtime_pb2


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
        

# TESTING
        
# pb_to_json("data/test")