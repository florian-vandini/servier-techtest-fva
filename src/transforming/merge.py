"""Generic transformation module"""

import json
import unicodedata
import pandas as pd
from utils.logger import logger
from utils.ut import TimestampEncoder

# pylint: disable=C0301
# pylint: disable=W1203
# pylint: disable=R0914


# Fonction pour enlever les accents d'une chaîne
def enlever_accents(chaine):
    """
    Removes accents from a given string.

    This function normalizes the input string to its decomposed form
    and removes any character that has the 'Mn' (Mark, Nonspacing) category
    according to Unicode character classification.

    Parameters:
        chaine (str): The string from which accents will be removed.

    Returns:
        str: A new string with all accents removed.
    """
    return "".join(
        c
        for c in unicodedata.normalize("NFD", chaine)
        if unicodedata.category(c) != "Mn"
    )


def merge_on_substring(df_1, df_2, column_1, column_2):
    """
    Merges two DataFrames based on whether a value in column_1 (df_1) is a substring of column_2 (df_2).

    Parameters:
    - df_1: The first DataFrame.
    - df_2: The second DataFrame.
    - column_1: The column name in df_1 (e.g., 'drug_name') to match as a substring.
    - column_2: The column name in df_2 (e.g., 'title') to search for substrings.

    Returns:
    - A DataFrame with merged rows where the value in column_1 is a substring of column_2.
    """
    # Create an empty list to hold merged rows
    merged_rows = []

    # Iterate through each row in df_1
    for _, row_1 in df_1.iterrows():
        # Find rows in df_2 where column_1 (drug_name) is a substring of column_2 (title)
        matching_rows = df_2[
            df_2[column_2].str.contains(row_1[column_1], case=False, na=False)
        ]

        # If there are matching rows, add them to the merged rows list
        for _, row_2 in matching_rows.iterrows():
            merged_row = pd.concat([row_1, row_2], axis=0)
            merged_rows.append(merged_row)

    # Combine all matched rows into a single DataFrame
    df_result = pd.DataFrame(merged_rows)

    return df_result


# Fonction générique pour relier médicaments et publications
def link_dfs(df_1, df_2, config_params):
    """
    Links two DataFrames based on a search column and a target column, nesting publications
    that mention the corresponding item.

    Parameters:
        df_1 (pd.DataFrame): The first DataFrame containing items (e.g., medicines) to be searched.
        df_2 (pd.DataFrame): The second DataFrame containing the publications with titles.
        config_params (dict): A dictionary containing the following keys:
            - search_column (str): The column in `df_1` to search for in `df_2`.
            - into_column (str): The column in `df_2` where the search will be conducted.
            - sub_level_name (str): The name for the sub-level where the nested publications will be added.
            - rm_accents (bool, optional): Whether to remove accents during the search (default is True).

    Returns:
        str: A JSON-formatted string containing the linked data, with publications nested under the corresponding item.
    """

    search_column = config_params["search_column"]
    into_column = config_params["into_column"]
    sub_level_name = config_params["sub_level_name"]
    rm_accents = config_params.get(
        "rm_accents", True
    )  # Default to True if not provided

    # Créer un dictionnaire vide pour stocker les résultats
    resultats = []

    # Parcours de chaque médicament dans df_1
    for _, item in df_1.iterrows():
        item_name = item[search_column]

        # Enlever les accents et mettre en minuscules pour la recherche
        if rm_accents:
            item_name = enlever_accents(item_name.lower())

        # Appliquer une fonction lambda pour vérifier si le médicament est dans le titre
        publications_mentionnees = df_2[
            df_2[into_column].apply(
                lambda titre, item_name=item_name: item_name
                in enlever_accents(titre.lower())
            )
        ]

        # Si des publications sont trouvées, les imbriquer dans un dictionnaire
        publications_list = []
        for _, publication in publications_mentionnees.iterrows():
            publication_dict = {}

            # Ajouter dynamiquement toutes les colonnes de publication, sauf la colonne du titre
            for col in df_2.columns:
                publication_dict[col] = publication[col]

            publications_list.append(publication_dict)

        # Create a dynamic dict from item
        item_dict = item.to_dict()
        item_dict[sub_level_name] = publications_list

        # Add the item with its nested publications to the result
        resultats.append(item_dict)
    logger.debug(f"Dict results: {resultats}")

    # Convert result as JSON
    json_result = json.dumps(
        resultats, indent=4, ensure_ascii=False, cls=TimestampEncoder
    )
    logger.debug(f"JSON result: {json_result}")

    return json_result


# medicaments_df = pd.DataFrame(
#     {
#         "medicament_id": [1, 2, 3],
#         "medicament_name": ["aspirine", "Paracétamol", "Ibuprofène"],
#     }
# )

# publications_df = pd.DataFrame(
#     {
#         "publication_id": [101, 102, 103],
#         "titre": [
#             "Effet de l'Aspirine sur la douleur",
#             "L'impact du Paracétamol vs l'aspirine",
#             "Étude de l'Ibuprofène pour les maux de tête",
#         ],
#     }
# )

# config = {
#     "search_column": "medicament_name",
#     "into_column": "titre",
#     "sub_level_name": "publications",
# }

# # Appeler la fonction générique
# results_df = link_dfs(medicaments_df, publications_df, config)
