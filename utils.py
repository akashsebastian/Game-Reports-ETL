import psycopg2
from datetime import date, timedelta


def connect_to_db():
    conn = psycopg2.connect(
        database="filmroomanalyticsdb",
        user="postgres",
        password="Jasmine#4044",
        host="database-cloud.cesoyytcmiho.ap-southeast-1.rds.amazonaws.com",
        port='5432'
    )
    return conn

def upload_rows(table, rows):
    try:
        conn = connect_to_db()
        query = f"INSERT INTO {table} VALUES %s"
        with conn.cursor() as cur:
            print(rows)
            args = [cur.mogrify('(%s, %s, %s, %s, %s, %s)', x).decode('utf-8')
        for x in rows]
            # execute_values(cur, query, rows)
            # conn.commit()
        return rows
    except Exception as e:
        print(e)
        return []

def delete_rows(table):
    conn = connect_to_db()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table}")
        conn.commit()

def get_dates():
    return date.today(), date.today() - timedelta(days=1)

def extract_int(num):
    return int(num) if num else 0

def divide(a, b):
    return a/b if b else 0