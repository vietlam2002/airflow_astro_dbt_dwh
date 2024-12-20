import yaml
import json
from datetime import datetime
from pymongo import MongoClient
import psycopg2
import os
import sys 

def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def connect_to_mongodb(config):
    client = MongoClient(config['MONGODB_CONNECTION_STRING'])
    print("OKLA")
    return client[config['MONGODB_DATABASE']]

def get_postgres_connection():
    conn = psycopg2.connect(
        host="14.225.205.7",     
        port=5432,            
        database="go_chat",   
        user="root",      
        password="root"  
    )
    print('OK!!')
    return conn
