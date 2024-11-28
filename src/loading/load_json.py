"""Cleaning module"""

# pylint: disable=C0301
# pylint: disable=W1203

import simplejson as json
import pandas as pd
from utils import ut
from utils.logger import logger


def load_json(
    filepath: str,
) -> pd.DataFrame:
    """
    loads a json file by handling duplicates, missing values, and date conversions.

    Args:
        filepath: Path to the json file.

    Returns:
        A Pandas DataFrame.
    """

    try:
        logger.info(f"Starting loading process for {filepath}")

        with open(filepath, "r", encoding="utf-8") as file:
            raw_json = file.read()  # Try to load the JSON data

        # Attempt to fix the malformed JSON
        raw_json = ut.clean_commas(raw_json)

        # Load DataFrame
        df = pd.DataFrame(json.loads(raw_json))

        logger.info(f"Loading process completed for {filepath}")
        logger.debug(f"DF debug {filepath} :\n {df.head(10)}")
        return df

    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON in file {filepath}: {e}")
        raise

    except Exception as e:
        logger.exception(f"An error occurred during the JSON loading process: {e}")
        raise


# Example Usage (Illustrative):
def custom_cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """Example custom cleaning function."""
    # Example: remove rows where a column value is greater than 10
    df = df[df["some_column"] <= 10]
    return df
