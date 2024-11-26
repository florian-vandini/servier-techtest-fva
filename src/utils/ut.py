"""Utils.json_ut module for json functions"""

import json
import re
from typing import List, Optional
import pandas as pd
from utils.logger import logger

# pylint: disable=C0301
# pylint: disable=W1203


class TimestampEncoder(json.JSONEncoder):
    """
    Custom encoder for Pandas Timestamp
    """

    def default(self, o):
        if isinstance(o, pd.Timestamp):
            return o.isoformat()  # Or any other format you need
        return super().default(o)


def clean_commas(json_string):
    """
    Removes extra commas in JSON objects and arrays.

    This function uses a regular expression to remove trailing commas before closing
    curly braces or square brackets in a JSON string, which can cause parsing issues.

    Args:
        json_string (str): The JSON string to clean.

    Returns:
        str: A cleaned JSON string with extra commas removed.
    """
    # Regular expression to remove extra commas in JSON objects and arrays
    cleaned_json_string = re.sub(r",\s*([\]}])", r"\1", json_string)
    return cleaned_json_string


def clean_dataframe(
    df: pd.DataFrame,
    dedupe_columns: Optional[List[str]] = None,
    dropna_subset: Optional[List[str]] = None,
    date_columns: Optional[List[str]] = None,
    custom_cleaning_func: Optional[callable] = None,
) -> pd.DataFrame:
    """
    A function to perform the common cleaning steps for both CSV and JSON data.

    :param df: The dataframe to clean.
    :param dedupe_columns: Columns to use for deduplication. If None, deduplicates based on all columns.
    :param dropna_subset: Subset of columns to check for NaN values. If None, no NaN check will be performed.
    :param date_columns: Columns to convert to datetime. If None, no date conversion will be performed.
    :param custom_cleaning_func: Custom cleaning function to apply. If None, no custom function will be applied.
    :return: Cleaned dataframe.
    """
    try:
        # Remove duplicates (if dedupe_columns is provided)
        if dedupe_columns:
            df.drop_duplicates(subset=dedupe_columns, inplace=True)
        else:
            df.drop_duplicates(inplace=True)

        # Remove NaN values (if dropna_subset is provided)
        if dropna_subset:
            df.dropna(subset=dropna_subset, inplace=True)

        # Convert date columns to datetime (if date_columns is provided)
        if date_columns:
            for col in date_columns:
                try:
                    df[col] = pd.to_datetime(df[col], format="mixed")
                except ValueError as e:  # Handle date parsing errors gracefully
                    logger.warning(
                        f"Error converting '{col}' to datetime: {e}.  Keeping original column."
                    )

        # Apply custom cleaning function (if provided)
        if custom_cleaning_func:
            df = custom_cleaning_func(df)

        return df

    except TypeError as e:
        logger.exception(f"Invalid input: {e}")
        raise

    except FileNotFoundError as e:
        logger.exception(f"File not found: {e}")
        raise

    except KeyError as e:
        logger.exception(f"Key error encountered: {e}")
        raise

    except Exception as e:
        logger.exception(
            f"An unexpected error occurred during the cleaning process: {e}"
        )
        raise
