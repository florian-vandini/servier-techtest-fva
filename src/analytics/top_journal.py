"""Export save module"""

import json
import pandas as pd
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
    # Group by the title and count the distinct drug names mentioned in each title
    title_drug_count = df.groupby(group_column)[drug_column].nunique()

    # Get the maximum number of distinct drugs mentioned
    max_drug_count = title_drug_count.max()

    # Get the titles that mention the maximum number of distinct drugs
    most_distinct_drug_titles = title_drug_count[
        title_drug_count == max_drug_count
    ].index.tolist()

    return most_distinct_drug_titles


def find_related_drugs(target_drug):
    # Trouver les journaux qui mentionnent le médicament donné
    related_journals = {
        journal for journal, drugs in journals.items() if target_drug in drugs
    }

    # Recueillir tous les médicaments mentionnés par ces journaux
    related_drugs = set()
    for journal in related_journals:
        related_drugs.update(journals[journal])

    # Retirer le médicament cible de la liste (si on ne veut pas l'inclure)
    related_drugs.discard(target_drug)

    return related_drugs
