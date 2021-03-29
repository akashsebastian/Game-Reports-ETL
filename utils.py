import psycopg2
from datetime import date, timedelta
import json


def connect_to_db():
    try:
        f = open('config.json',)
        data = json.load(f)
        conn = psycopg2.connect(
            database=data['database'],
            user=data['user'],
            password=data['password'],
            host=data['host'],
            port='5432'
        )
        return conn
    except e:
        print(f"Couldn't connect to db. Error: {e}")

def divide(a, b):
    return a/b if b else 0
