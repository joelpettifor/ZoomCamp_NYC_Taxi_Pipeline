import pandas as pd  
import argparse
from sqlalchemy import create_engine



def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    data = pd.read_parquet(url)

    data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
    data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)

    data.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    data.to_sql(name=table_name, con=engine, if_exists='append')

    print("completed")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet files to Postgres')

    parser.add_argument('--user', help='Postgres user name')
    parser.add_argument('--password', help='Postgres password')
    parser.add_argument('--host', help='Postgres host')
    parser.add_argument('--port', help='Postgres port')
    parser.add_argument('--db', help='Postgres database name')
    parser.add_argument('--table_name', help='Postgres table name')
    parser.add_argument('--url', help='URL of the parquet file')


    args = parser.parse_args()

    main(args)