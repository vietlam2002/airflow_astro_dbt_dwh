from pymongo import MongoClient
import pandas as pd
import json
import os
import sys
from datetime import datetime
from bson import json_util, ObjectId

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '..', 'config.yaml')
utils_path = os.path.join(current_dir, '..', 'utils')
sys.path.append(utils_path)
from cache_utils import load_config, connect_to_mongodb, get_postgres_connection

def convert_objectid_to_string(data):
    for item in data:
        for key, value in item.items():
            if isinstance(value, ObjectId):
                item[key] = str(value)
            elif isinstance(value, list):
                item[key] = ";".join([str(v) for v in value])
    return data
def load_data_mongo():
    config = load_config(config_path)
    create_config = config['create']
    db = connect_to_mongodb(create_config)

    collection = db['projects']
    query = {
        "updatedAt": {"$gt": '2024-11-26 06:03:00'},
        "createdAt": {"$lte": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}
    }
    documents = collection.find(query)
    data = list(documents)
    data_converted = convert_objectid_to_string(data)
    df = pd.DataFrame(data_converted)
    print(f"{len(df)} rows retrieved")
    return df
