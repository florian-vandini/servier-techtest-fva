# Data Pipeline Project

This project is a Python-based data pipeline designed to process, transform, and load data into a target system (e.g., database, data warehouse, etc.). The pipeline consists of multiple stages, such as data extraction, transformation, and loading (ETL), aimed at automating data workflows for analytical purposes.

## Table of Contents
- [Project Overview](#project-overview)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Dependencies](#dependencies)
- [Pour aller plus loin](#Pour-aller-plus-loin)
- [SQL](#SQL)

## Project Overview

The data pipeline is designed to:
1. Extract data from various sources (e.g., APIs, databases, CSV files).
2. Transform data (e.g., cleaning, filtering, aggregating).
3. Load transformed data into a data destination (e.g., PostgreSQL, Google BigQuery).

The goal of this pipeline is to ensure that data is processed efficiently, accurately, and in a scalable manner. The pipeline can be customized and extended based on your needs.

## Installation

To install the necessary dependencies for the project, follow these steps:

1. **Clone the repository:**

   ```bash
   git clone https://github.com/florian-vandini/servier-techtest-fva.git
   cd servier-techtest-fva

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt

## Usage

To run the data pipeline, follow these steps:

1. **Set up configuration:**



2. **Run the pipeline:**
   ```bash
   python src/main.py

## Project Structure

    server-techtest-fva/
    ├── data/                        # Directory for storing data files
    │   ├── raw/                     # Raw input data (e.g., CSV, JSON files)
    │   └── export/                  # Processed/exported data (e.g., cleaned, merged files)
    ├── src/                         # Source code directory
    │   ├── cleaning/                # Data cleaning module
    │   │   └── clean_df.py          # Function to clean DataFrame
    │   ├── loading/                 # Data loading module
    │   │   ├── load_csv.py          # Function to load CSV files
    │   │   └── load_json.py         # Function to load JSON files
    │   ├── transforming/            # Data transformation module
    │   │   └── merge.py             # Functions to merge data
    │   ├── exporting/               # Data exporting module
    │   │   └── save.py              # Function to save data (e.g., to a database, CSV, etc.)
    │   ├── analytics/               # Data analysis module
    │   │   └── top_journal.py       # Functions for journal and drug analysis
    │   ├── utils/                   # Utility functions
    │   │   ├── logger.py            # Logging setup and configurations
    │   │   └── ut.py                # Utility functions (e.g., helper functions for tests or other utilities)
    │   └── main.py                  # Entry point for running the pipeline
    ├── requirements.txt             # Python dependencies for the project
    └── README.md                    # Project documentation

## Dependencies

    pandas==2.2.3         # Data manipulation and analysis library
    pylint==3.3.1         # Python static code analysis tool
    black==24.10.0        # Python code formatter
    simplejson==3.19.3    # Simple JSON encoder and decoder

## Pour aller plus loin

### Par retour de mail (ou directement sur le repo git si vous le souhaitez), vous pouvez répondre aux questions suivantes (ne nécessite pas d’implémentation dans votre projet) :

### Quels sont les éléments à considérer pour faire évoluer votre code afin qu’il puisse gérer de grosses volumétries de données (fichiers de plusieurs To ou millions de fichiers par exemple) ?

### Pourriez-vous décrire les modifications qu’il faudrait apporter, s’il y en a, pour prendre en considération de telles volumétries ?

    1. Move to cloud: Si la confidentialité et la législation le permet, prévoir une migration full Cloud afin de bénéficier d'instances de run hautement scalables avec une maitrise des coûts.
       Remplacer les dossiers en local par des buckets Cloud Storage, ou privilégier un autre format d'input pour les données (Streaming event via Kafka, batch transfert, ou bien en fichiers parquets avec chargement incrémental dans la mesure du possible).
    2. Migration vers un code distribué en parallèle : Garder la même structure de code et les functions mais sur une library avec distribution du calcul (type Dask, Apache Beam, ou pyspark à défaut).
    3. Dockerisation et déploiement sur une instance parallélisée: Dockeriser le code avec une image OS de faible taille et prévoir son déploiement sur une instance Cloud Run pour les moyens volumes ou bien GKE.
    4. Gestion des logs plus fine avec association de data quality pour maitriser les données rejettées et avoir un visuel précis sur le pipeline.
    5. Prévoir une rétention des données rejettées type deadletter queue ou a minima un historique des données brutes reçues (Cold archive bucket, ou backup global récurent). Ces données doivent être faciles à rejouer à la demande.


# SQL

   ```sql
   select 
      date
      , sum(prod_price*prod_qty) as ventes
   from
      transactions
   where
      date between "01/01/20" and "31/12/20"
   group by 
      date
   order by
      date asc

---------------

   with ventes_produits as (
      select 
         t.*
         , pn.product_type 
      from
         transactions t
      left join 
         product_nomenclature pn on t.prod_id = pn.product_id
   )

   select 
      client_id
      , sum( if( product_type='MEUBLE', prod_price*prod_qty, 0)) as ventes_meuble
      , sum( if( product_type='DECO', prod_price*prod_qty, 0)) as ventes_deco
   from
      ventes_produits
   where
      date between "01/01/19" and "31/12/19"
   group by 
      client_id