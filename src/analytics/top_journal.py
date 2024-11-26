"""Export save module"""

from utils.logger import logger

# pylint: disable=C0301
# pylint: disable=W1203
# pylint: disable=R0914


def get_best_journal(df, group_column, drug_column):
    """
    Returns the title(s) that mention the most distinct drug names.

    Parameters:
    - df: The pandas DataFrame containing the data.
    - group_column: The name of the column containing newspapper name.
    - drug_column: The name of the column containing drug names.

    Returns:
    - A list of the title(s) that mention the most distinct drug names.
    """
    logger.info(f"Getting best journal for {group_column} and {drug_column}")

    # Group by the title and count the distinct drug names mentioned in each title
    title_drug_count = df.groupby(group_column)[drug_column].nunique()

    # Get the maximum number of distinct drugs mentioned
    max_drug_count = title_drug_count.max()

    # Get the titles that mention the maximum number of distinct drugs
    most_distinct_drug_titles = title_drug_count[
        title_drug_count == max_drug_count
    ].index.tolist()

    return most_distinct_drug_titles


def find_related_drugs(df, target_drug):
    """
    Fonction pour filtrer le DataFrame par le nom du médicament
    """

    # Filter on target drug
    filtered_df = df[df["drug"] == target_drug]

    # Get related drugs based on the same journals mentionning the target drug
    related_drugs_df = df[df["journal"].isin(filtered_df["journal"])]

    return related_drugs_df
