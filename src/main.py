"""Main module"""

# pylint: disable=C0301
# pylint: disable=W1203

import pandas as pd

from cleaning.clean_df import clean_df
from loading.load_csv import load_csv
from loading.load_json import load_json
from transforming.merge import link_dfs, merge_on_substring
from exporting.save import save
from analytics.top_journal import get_best_journal, find_related_drugs

# from transformation import merge_data
from utils.logger import logger


def main():
    """Execute the main data pipeline."""

    logger.info("Starting data pipeline.")

    # OPTIONNAL config for link_dfs function
    config = {
        "search_column": "drug",
        "into_column": "title",
        "sub_level_name": "publications",
    }

    try:
        #####################
        # Loading Zone
        drugs_df = load_csv(
            "./data/raw/drugs.csv",
            sep=",",
        )

        pubmed_csv_df = load_csv(
            "./data/raw/pubmed.csv",
            sep=",",
        )

        clinical_trials_df = load_csv(
            "./data/raw/clinical_trials.csv",
            sep=",",
        )

        pubmed_json_df = load_json(
            "./data/raw/pubmed.json",
        )

        #####################
        # Cleaning Zone

        drugs_df = clean_df(drugs_df)
        pubmed_csv_df = clean_df(pubmed_csv_df, date_columns=["date"])
        clinical_trials_df = clean_df(clinical_trials_df, date_columns=["date"])
        pubmed_json_df = clean_df(pubmed_json_df, date_columns=["date"])

        #####################
        # Transformation Zone

        # Concatenate pubmed dfs
        pubmed_df = pd.concat([pubmed_csv_df, pubmed_json_df], ignore_index=True)
        pubmed_df = clean_df(
            pubmed_df
        )  # Clean after concatenation to remove potential duplicates

        print(pubmed_df.head(100))

        merged_drug_pub_df = merge_on_substring(
            drugs_df, pubmed_df, column_1="drug", column_2="title"
        )

        merged_drug_clinic_df = merge_on_substring(
            drugs_df, clinical_trials_df, column_1="drug", column_2="scientific_title"
        )

        result_df = pd.concat(
            [merged_drug_pub_df, merged_drug_clinic_df], ignore_index=True
        )

        # OPTIONAL
        results_df = link_dfs(drugs_df, pubmed_df, config)
        logger.debug(results_df)

        #####################
        # Exporting Zone

        save(clean_df(result_df, date_columns=["date"]), "result.json")

        #####################
        # Analytics Zone

        adhoc_df = load_json(
            "./data/export/result.json",
        )

        adhoc_df["title"] = adhoc_df["title"].astype(str) + adhoc_df[
            "scientific_title"
        ].astype(str)

        print(adhoc_df)

        print(get_best_journal(adhoc_df, "journal", "drug"))
        # ----------------
        adhoc_df = load_json(
            "./data/export/result.json",
        )
        adhoc_df = adhoc_df[
            ~adhoc_df["title"].isin(["none"]) & adhoc_df["title"].notna()
        ]
        print(find_related_drugs(adhoc_df, "TETRACYCLINE"))

        logger.info("Pipeline ended successfuly")

    except Exception as e:
        logger.exception(f"An error occured during pipeline execution: {e}")
        raise


if __name__ == "__main__":
    main()
