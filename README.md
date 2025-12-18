# Collecte_de_donnees_ateliers

## Atelier 1 : ingestion et préparation de données open data

### Sources utilisées
- État du trafic : Bordeaux Métropole DataHub
- Incidents routiers : Bordeaux Métropole / DIRA
- Vacances scolaires et jours fériés : data.gouv.fr

### Objectif
- Mettre en place un datalake local
- Stocker les données au format Parquet
- Produire un jeu de features prêt à être utilisé par un modèle de machine learning

### Exécution
python scripts/ingest_atelier1.py --run all

### Résultats
- Données brutes historisées (datalake/raw)
- Données nettoyées et normalisées (datalake/clean)
- Jeu de données final de features (datalake/features)


## Atelier 2 : collecte en ligne (scraping) de données touristiques

### Objectif
Enrichir les données existantes avec des informations touristiques collectées en ligne (agenda d’événements).

### Précautions
Collecte limitée en fréquence
Données publiques uniquement
Usage strictement pédagogique
Remplacement du domaine réel par tourisme.example avant stockage

### Étapes
Récupération d’une liste d’événements (titre, date, lien)
Exploration des pages détail pour enrichir les informations
Stockage des données en Parquet avec nommage unique

### Exécution
python scripts/scrape_list.py
python scripts/scrape_details.py

### Installation de l’environnement
pip install dotenv

### Installation
pip install -r requirements.txt

### Formats de données
Toutes les données sont stockées au format Parquet
Les fichiers bruts et intermédiaires ne sont pas versionnés (via .gitignore)


## Auteurs
Edouard Dieppois
Nicolas Chiche
Thomas Crusel
>>>>>>> 2ff411dd7c4b1e4a22f96580d34650b2128205c1
