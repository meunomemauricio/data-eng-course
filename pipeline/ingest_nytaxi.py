"""Data Ingestion script for NY Taxi Dataset."""

import click
import pandas as pd
from click.exceptions import Exit
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DTYPE = {
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
    "congestion_surcharge": "float64",
}

DATE_FIELDS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

TRIP_BASE_URL = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
)
ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)

TRIP_TABLE = "taxi_trips"
LOOKUP_TABLE = "zones"


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="pgdatabase", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option(
    "--target-table",
    default="",
    help="Target table name",
)
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    url = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

    engine = _get_db_engine(url=url)
    _ingest_trip_data(engine=engine)
    _ingest_zone_lookup(engine=engine)


def _get_db_engine(url: str) -> Engine:
    print("Engine URL:", url)
    engine = create_engine(url=url)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except SQLAlchemyError as e:
        print("DB not reachable:", e)
        raise Exit()

    return engine


def _ingest_trip_data(engine: Engine):
    print("Ingesting NY Taxi Trip Dataset")

    dataset_url = f"{TRIP_BASE_URL}/yellow_tripdata_2021-01.csv.gz"
    print("Trip Dataset URL:", dataset_url)

    df_iter = pd.read_csv(
        dataset_url,
        dtype=DTYPE,
        parse_dates=DATE_FIELDS,
        iterator=True,
        chunksize=100000,
    )

    first = True
    for df_chunk in df_iter:
        if first:
            df_chunk.head(0).to_sql(
                name=TRIP_TABLE, con=engine, if_exists="replace"
            )
            first = False
            print("Table Created")

        df_chunk.to_sql(name=TRIP_TABLE, con=engine, if_exists="append")
        print("Inserted", len(df_chunk))


def _ingest_zone_lookup(engine: Engine):
    print("Ingesting NY Zone Lookup Table")
    df = pd.read_csv(ZONE_LOOKUP_URL)
    df.to_sql(name=LOOKUP_TABLE, con=engine, if_exists="replace")


if __name__ == "__main__":
    main()
