import argparse
import pandas as pd
from data_extraction import wait_for_postgres
from sqlalchemy import create_engine, text

def main(params):
    file_path = params.file_path
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    print("Loading sample for schema inference...")
    sample_df = pd.read_csv(
        file_path,
        nrows=100,
        low_memory=False
    )

    print("DataFrame head:")
    print(sample_df.head())

    ddl = pd.io.sql.get_schema(sample_df, name=table_name)
    ddl = ddl.replace(
        'CREATE TABLE',
        'CREATE TABLE IF NOT EXISTS'
    )

    print("Data definition language of the table: ")
    print(ddl)

    # creating the empty table in postgres using the data definition language of the table generated above
    wait_for_postgres(user, password, host, port, db)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("Data definition language of the table: ")
    print(ddl)

    # copying the contents of the csv file into the table created above
    raw_conn = engine.raw_connection()
    cur = raw_conn.cursor()
    print("Starting COPY from CSV...")
    with open(file_path, 'r') as f:
        copy_sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER"
        cur.copy_expert(copy_sql, f)
        raw_conn.commit()
    cur.close()
    raw_conn.close()

    print("Data ingestion complete.")



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load CSV into Postgres with proper datetime types')
    parser.add_argument('file_path', help='filepath of data source')
    parser.add_argument('user', help='Database user')
    parser.add_argument('password', help='Database password')
    parser.add_argument('host', help='Database host')
    parser.add_argument('port', help='Database port')
    parser.add_argument('db', help='Database name')
    parser.add_argument('table_name', help='Target table name')
    args = parser.parse_args()
    main(args)