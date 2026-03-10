"""
transform.py
------------
Cleans, validates, and enriches raw sales data.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicates, strip whitespace, and standardise column names.
    """
    logger.info("Cleaning data...")
    original_rows = len(df)

    # Normalise column names
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "", regex=True)
    )

    # Drop full duplicates
    df = df.drop_duplicates()

    # Strip string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

    logger.info(f"Removed {original_rows - len(df):,} duplicate rows.")
    return df


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast columns to appropriate data types.
    """
    logger.info("Casting data types...")

    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    if "ship_date" in df.columns:
        df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

    numeric_cols = ["sales", "quantity", "discount", "profit", "unit_price"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows that fail business validation rules.
    """
    logger.info("Validating data...")
    before = len(df)

    if "order_id" in df.columns:
        df = df.dropna(subset=["order_id"])

    if "sales" in df.columns:
        df = df[df["sales"] > 0]

    if "quantity" in df.columns:
        df = df[df["quantity"] >= 1]

    logger.info(f"Removed {before - len(df):,} invalid rows after validation.")
    return df


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns useful for analytics.
    """
    logger.info("Enriching data with derived columns...")

    if "order_date" in df.columns:
        df["order_year"]    = df["order_date"].dt.year
        df["order_month"]   = df["order_date"].dt.month
        df["order_quarter"] = df["order_date"].dt.quarter
        df["order_dow"]     = df["order_date"].dt.day_name()

    if "sales" in df.columns and "profit" in df.columns:
        df["profit_margin"] = (df["profit"] / df["sales"]).round(4)

    if "sales" in df.columns and "quantity" in df.columns:
        df["revenue_per_unit"] = (df["sales"] / df["quantity"]).round(2)

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full transformation pipeline: clean -> cast -> validate -> enrich.
    """
    df = clean_data(df)
    df = cast_types(df)
    df = validate(df)
    df = enrich(df)
    logger.info(f"Transformation complete. Final shape: {df.shape}")
    return df
