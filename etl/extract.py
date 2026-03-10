"""
extract.py
----------
Handles data extraction from CSV files and SQL databases.
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def extract_csv(filepath: str) -> pd.DataFrame:
    """
    Extract data from a CSV file.

    Args:
        filepath: Path to the CSV file.

    Returns:
        DataFrame with raw data.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    logger.info(f"Extracting data from {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Extracted {len(df):,} rows and {len(df.columns)} columns.")
    return df


def extract_sql(engine, query: str) -> pd.DataFrame:
    """
    Extract data from a SQL database using SQLAlchemy engine.

    Args:
        engine: SQLAlchemy engine object.
        query:  SQL SELECT query string.

    Returns:
        DataFrame with query results.
    """
    logger.info("Extracting data from SQL source.")
    df = pd.read_sql(query, engine)
    logger.info(f"Extracted {len(df):,} rows.")
    return df
