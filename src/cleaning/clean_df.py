"""Cleaning module"""

# pylint: disable=C0301
# pylint: disable=W1203
# pylint: disable=W1309

from typing import List, Optional
import pandas as pd
from utils import ut
from utils.logger import logger


def clean_df(
    df: pd.DataFrame,
    dedupe_columns: Optional[List[str]] = None,
    dropna_subset: Optional[List[str]] = None,
    date_columns: Optional[List[str]] = None,
    custom_cleaning_func=None,  # Optional custom cleaning function
) -> pd.DataFrame:
    """
    Cleans a CSV file by handling duplicates, missing values, and date conversions.

    Args:
        df: Pandas DataFrame to clean.
        dedupe_columns: List of columns to consider for deduplication. If None, deduplicates based on all columns.
        dropna_subset: List of columns to check for missing values. Rows with missing values in these columns are dropped.
        date_columns: List of columns to convert to datetime objects.
        custom_cleaning_func: An optional custom cleaning function that takes the DataFrame as input and returns the cleaned DataFrame.

    Returns:
        A cleaned Pandas DataFrame or None if an error occurred during cleaning.
    """

    try:
        logger.info(f"Starting cleaning process")

        # Clean the DataFrame using the common cleaning function
        df = ut.clean_dataframe(
            df, dedupe_columns, dropna_subset, date_columns, custom_cleaning_func
        )

        logger.info(f"Cleaning process completed")
        logger.debug(f"DF debug {df} :\n{df.head(10)}")

        return df

    except Exception as e:
        logger.exception(f"An error occurred during CSV loading process: {e}")
        raise


# Example Usage (Illustrative):
def custom_cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """Example custom cleaning function."""
    # Example: remove rows where a column value is greater than 10
    df = df[df["some_column"] <= 10]
    return df
