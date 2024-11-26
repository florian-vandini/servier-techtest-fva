"""Export save module"""

from utils.logger import logger

# pylint: disable=C0301
# pylint: disable=W1203
# pylint: disable=R0914


def save(df, filename):
    """
    Saves a DataFrame as a JSON file.

    Parameters:
    - df: The DataFrame to be saved.
    - filename: The name of the file to save the DataFrame to.
    """
    if df.empty:
        logger.warning("DataFrame is empty, nothing to save.")
    else:
        filepath = f"data/export/{filename}"
        df.to_json(filepath, orient="records", lines=False)
        logger.info(f"Data saved to {filepath}")
