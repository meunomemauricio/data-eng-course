"""Data Ingestion script for NY Taxi Dataset."""

import click
import pandas as pd
from click.exceptions import Exit
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

TRIPS_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "green_tripdata_2025-11.parquet"
)
ZONE_LOOKUP_URL = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/"
    "taxi_zone_lookup.csv"
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
    print("Trip Dataset URL:", TRIPS_URL)
    df = pd.read_parquet(TRIPS_URL)
    df.to_sql(name=TRIP_TABLE, con=engine, if_exists="replace")
    print("Trip data imported.")


def _ingest_zone_lookup(engine: Engine):
    print("Ingesting NY Zone Lookup Table")
    print("Zone Data URL:", ZONE_LOOKUP_URL)
    df = pd.read_csv(ZONE_LOOKUP_URL)
    df.to_sql(name=LOOKUP_TABLE, con=engine, if_exists="replace")
    print("Zone data imported.")


if __name__ == "__main__":
    main()
