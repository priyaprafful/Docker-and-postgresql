#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import ssl
import certifi
import urllib.request
import click

# SSL context using certifi
ssl_context = ssl.create_default_context(cafile=certifi.where())

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> pd.DataFrame:
    # Use urllib to open URL with SSL context
    with urllib.request.urlopen(url, context=ssl_context) as response:
        df_iter = pd.read_csv(
            response,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize,
            compression='gzip'
        )

        first_chunk = next(df_iter)

        first_chunk.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists="replace"
        )
        print(f"Table {target_table} created")

        first_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted first chunk: {len(first_chunk)}")

        for df_chunk in tqdm(df_iter):
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append"
            )
            print(f"Inserted chunk: {len(df_chunk)}")

    print(f'done ingesting to {target_table}')


@click.command()
@click.option("--pg-user", required=True, help="Postgres username")
@click.option("--pg-pass", required=True, help="Postgres password")
@click.option("--pg-host", required=True, help="Postgres host")
@click.option("--pg-port", default=5432, help="Postgres port")
@click.option("--pg-db", required=True, help="Postgres database name")
@click.option("--year", default=2021, help="Year of dataset")
@click.option("--month", default=1, help="Month of dataset")
@click.option("--chunksize", default=100000, help="Number of rows per chunk")
@click.option("--target-table", default="yellow_taxi_data", help="Target table name")
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )


if __name__ == '__main__':
    main()
