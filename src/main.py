
import logging
import pandas as pd
# from src.loading import load_drugs, load_pubmed_csv, load_pubmed_json, load_clinical_trials
from src.cleaning import clean_csv
# from src.transformation import merge_data
# from src.utils.logging import logging

# Configure logging
logging()
logger = logging.getLogger(__name__)


def main():
    """Exécute le pipeline de données."""

    logger.info("Démarrage du pipeline de données.")

    try:
        # Chargement des données
        drugs_df = load_drugs("data/raw/drugs.csv")
        pubmed_csv_df = load_pubmed_csv("data/raw/pubmed.csv")
        pubmed_json_df = load_pubmed_json("data/raw/pubmed.json")
        clinical_trials_df = load_clinical_trials("data/raw/clinical_trials.csv")


        # Nettoyage des données
        drugs_df = clean_drugs(drugs_df)
        pubmed_csv_df = clean_pubmed_data(pubmed_csv_df)
        pubmed_json_df = clean_pubmed_data(pubmed_json_df) # Same cleaning function can be used
        clinical_trials_df = clean_clinical_trials(clinical_trials_df)

        # Concaténer les DataFrames PubMed
        pubmed_df = pd.concat([pubmed_csv_df, pubmed_json_df], ignore_index=True)
        pubmed_df = clean_pubmed_data(pubmed_df) # Clean after concatenation to remove potential duplicates

        # Transformation et fusion des données
        merged_df = merge_data(drugs_df, pubmed_df, clinical_trials_df)


        # Sauvegarde des données traitées
        merged_df.to_csv("data/processed/merged_data.csv", index=False)

        logger.info("Pipeline de données terminé avec succès.")

    except Exception as e:
        logger.exception(f"Une erreur s'est produite: {e}")


if __name__ == "__main__":
    main()

