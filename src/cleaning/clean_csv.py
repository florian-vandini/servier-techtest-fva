
import pandas as pd
import logging
from typing import List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def clean_csv(
    filepath: str,
    dedupe_columns: Optional[List[str]] = None,
    dropna_subset: Optional[List[str]] = None,
    date_columns: Optional[List[str]] = None,
    custom_cleaning_func=None,  # Optional custom cleaning function
    **kwargs,  # Additional keyword arguments for pd.read_csv
) -> pd.DataFrame:
    """
    Cleans a CSV file by handling duplicates, missing values, and date conversions.

    Args:
        filepath: Path to the CSV file.
        dedupe_columns: List of columns to consider for deduplication. If None, deduplicates based on all columns.
        dropna_subset: List of columns to check for missing values. Rows with missing values in these columns are dropped.
        date_columns: List of columns to convert to datetime objects.
        custom_cleaning_func: An optional custom cleaning function that takes the DataFrame as input and returns the cleaned DataFrame.
        **kwargs: Additional keyword arguments to pass to pd.read_csv.

    Returns:
        A cleaned Pandas DataFrame or None if an error occurred during cleaning.
    """

    try:
        logger.info(f"Starting cleaning process for {filepath}")

        # Load the CSV file
        df = pd.read_csv(filepath, **kwargs)

        # Remove duplicates
        if dedupe_columns:
            df.drop_duplicates(subset=dedupe_columns, inplace=True)
        else:
            df.drop_duplicates(inplace=True)

        # Handle missing values
        if dropna_subset:
            df.dropna(subset=dropna_subset, inplace=True)


        # Convert date columns to datetime
        if date_columns:
            for col in date_columns:
                try:
                    df[col] = pd.to_datetime(df[col])
                except ValueError as e:  # Handle date parsing errors gracefully
                    logger.warning(f"Error converting '{col}' to datetime: {e}.  Keeping original column.")

        # Apply custom cleaning function (if provided)
        if custom_cleaning_func:
            df = custom_cleaning_func(df)

        logger.info(f"Cleaning process completed for {filepath}")
        return df

    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return None
    except Exception as e:
        logger.exception(f"An error occurred during the cleaning process: {e}")
        return None



# Example Usage (Illustrative):
def custom_cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """Example custom cleaning function."""
    # Example: remove rows where a column value is greater than 10
    df = df[df['some_column'] <= 10]
    return df

cleaned_df = clean_csv(
    "path/to/your/file.csv",
    dedupe_columns=['column1', 'column2'],
    dropna_subset=['important_column'],
    date_columns=['date_column'],
    custom_cleaning_func=custom_cleaner, # Include your custom cleaning function
    sep=";", # Example for a semicolon-separated CSV
)

if cleaned_df is not None:
    # Proceed with your data processing
    print(cleaned_df.head())

