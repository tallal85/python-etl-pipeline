"""
load.py
-------
Loads transformed data into a SQLite (or any SQLAlchemy-compatible) database
and optionally exports to CSV / Parquet.
"""

import pandas as pd
import logging
from pathlib import Path
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def load_to_sql(
    df: pd.DataFrame,
    table_name: str,
    db_url: str = "sqlite:///data/warehouse.db",
    if_exists: str = "replace",
    index: bool = False,
) -> None:
    """
    Load a DataFrame into a SQL table.

    Args:
        df:         Transformed DataFrame.
        table_name: Target table name.
        db_url:     SQLAlchemy connection string.
        if_exists:  'replace', 'append', or 'fail'.
        index:      Whether to write the DataFrame index as a column.
    """
    logger.info(f"Loading {len(df):,} rows into table '{table_name}'...")
    engine = create_engine(db_url)
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=index)
    logger.info(f"Load complete -> {db_url} :: {table_name}")


def load_to_csv(df: pd.DataFrame, filepath: str) -> None:
    """Export DataFrame to CSV."""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False)
    logger.info(f"Exported {len(df):,} rows to {filepath}")


def load_to_parquet(df: pd.DataFrame, filepath: str) -> None:
    """Export DataFrame to Parquet (columnar, efficient for large datasets)."""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filepath, index=False)
    logger.info(f"Exported {len(df):,} rows to {filepath}")


def verify_load(table_name: str, db_url: str = "sqlite:///data/warehouse.db") -> int:
    """Return the row count of a loaded table for quick sanity checking."""
    engine = create_engine(db_url)
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = result.scalar()
    logger.info(f"Verification: '{table_name}' contains {count:,} rows.")
    return count
