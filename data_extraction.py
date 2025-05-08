import argparse
import gzip
import shutil
import os
import time
import requests
import pandas as pd
import psycopg2
import re
from psycopg2 import OperationalError
from sqlalchemy import create_engine, text


def download_file(url, output_path):
    print(f"Downloading file from {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        print(f"File downloaded: {output_path}")
    else:
        raise Exception(f"Failed to download file: Status {response.status_code}")


def extract_gzip(source_path, dest_path):
    print(f"Extracting {source_path}...")
    with gzip.open(source_path, 'rb') as f_in:
        with open(dest_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Extracted to: {dest_path}")


def wait_for_postgres(user, password, host, port, db):
    print("Waiting for PostgreSQL to be ready...")
    for _ in range(10):
        try:
            conn = psycopg2.connect(
                dbname=db,
                user=user,
                password=password,
                host=host,
                port=port
            )
            conn.close()
            print("PostgreSQL is ready.")
            return
        except OperationalError:
            print("PostgreSQL not ready yet, retrying...")
            time.sleep(3)
    raise Exception("PostgreSQL not reachable after multiple attempts.")


def main(params):
    download_link = params.download_link
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    gzip_file = "downloaded_data.csv.gz"
    csv_file = "extracted_data.csv"

    # Download and extract
    if not os.path.exists(gzip_file):
        download_file(download_link, gzip_file)
    else:
        print("GZIP file already exists.")
    if not os.path.exists(csv_file):
        extract_gzip(gzip_file, csv_file)
    else:
        print("CSV file already extracted.")

    # Infer schema with proper datetime parsing
    print("Loading sample for schema inference...")
    sample_df = pd.read_csv(
        csv_file,
        nrows=100,
        low_memory=False,
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    )
    ddl = pd.io.sql.get_schema(sample_df, name=table_name)
    ddl = ddl.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
    # Override datetime columns to TIMESTAMP
    ddl = re.sub(
        r'"tpep_pickup_datetime" TEXT',
        '"tpep_pickup_datetime" TIMESTAMP',
        ddl
    )
    ddl = re.sub(
        r'"tpep_dropoff_datetime" TEXT',
        '"tpep_dropoff_datetime" TIMESTAMP',
        ddl
    )
    print("Generated DDL:")
    print(ddl)

    # Create table in Postgres
    wait_for_postgres(user, password, host, port, db)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    with engine.begin() as conn:
        conn.execute(text(ddl))

    # Bulk load via COPY
    raw_conn = engine.raw_connection()
    cur = raw_conn.cursor()
    print("Starting COPY from CSV...")
    with open(csv_file, 'r') as f:
        copy_sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER"
        cur.copy_expert(copy_sql, f)
        raw_conn.commit()
    cur.close()
    raw_conn.close()

    print("Data ingestion complete.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load CSV into Postgres with proper datetime types')
    parser.add_argument('download_link', help='URL of the .csv.gz file')
    parser.add_argument('user', help='Database user')
    parser.add_argument('password', help='Database password')
    parser.add_argument('host', help='Database host')
    parser.add_argument('port', help='Database port')
    parser.add_argument('db', help='Database name')
    parser.add_argument('table_name', help='Target table name')
    args = parser.parse_args()
    main(args)