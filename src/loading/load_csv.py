"""Loading module"""

# pylint: disable=C0301
# pylint: disable=W1203

import pandas as pd
from utils.logger import logger


def load_csv(
    filepath: str,
    **kwargs,  # Additional keyword arguments for pd.read_csv
) -> pd.DataFrame:
    """
    Loads a CSV file into DataFrame.

    Args:
        filepath: Path to the CSV file.
        **kwargs: Additional keyword arguments to pass to pd.read_csv.

    Returns:
        A Pandas DataFrame.
    """

    try:
        logger.info(f"Starting loading process for {filepath}")

        # Load the CSV file
        df = pd.read_csv(filepath, **kwargs)

        logger.info(f"Loading process completed for {filepath}")
        logger.info(f"DF debug {filepath} :\n{df.head(10)}")

        return df

    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise
    except Exception as e:
        logger.exception(f"An error occurred during CSV loading process: {e}")
        raise
