"""
pipeline.py
-----------
Orchestrates the full Extract -> Transform -> Load pipeline.

Usage:
    python -m etl.pipeline --source data/sample_sales.csv --table sales_fact
"""

import argparse
import logging
import time

from etl.extract import extract_csv
from etl.transform import transform
from etl.load import load_to_sql, load_to_parquet, verify_load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline(
    source: str,
    table: str = "sales_fact",
    db_url: str = "sqlite:///data/warehouse.db",
    export_parquet: bool = True,
) -> None:
    start = time.time()
    logger.info("=" * 50)
    logger.info("ETL PIPELINE STARTED")
    logger.info("=" * 50)

    # --- EXTRACT ---
    df_raw = extract_csv(source)

    # --- TRANSFORM ---
    df_clean = transform(df_raw)

    # --- LOAD ---
    load_to_sql(df_clean, table_name=table, db_url=db_url)

    if export_parquet:
        parquet_path = f"data/{table}.parquet"
        load_to_parquet(df_clean, parquet_path)

    # --- VERIFY ---
    verify_load(table, db_url)

    elapsed = round(time.time() - start, 2)
    logger.info("=" * 50)
    logger.info(f"ETL PIPELINE COMPLETE in {elapsed}s")
    logger.info("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the sales ETL pipeline.")
    parser.add_argument("--source", default="data/sample_sales.csv", help="Path to input CSV")
    parser.add_argument("--table",  default="sales_fact",            help="Target SQL table name")
    parser.add_argument("--db",     default="sqlite:///data/warehouse.db", help="SQLAlchemy DB URL")
    parser.add_argument("--no-parquet", action="store_true", help="Skip Parquet export")
    args = parser.parse_args()

    run_pipeline(
        source=args.source,
        table=args.table,
        db_url=args.db,
        export_parquet=not args.no_parquet,
    )
