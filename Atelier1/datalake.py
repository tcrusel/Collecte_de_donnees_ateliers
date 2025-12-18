from __future__ import annotations
import os
from datetime import datetime, timezone
import pandas as pd


# ============================
# Définition des répertoires
# ============================

# Répertoire où se trouve le script (chemin absolu)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Répertoire contenant les fichiers CSV sources
DATA_DIR = os.path.join(BASE_DIR, "data")

# Répertoire de sortie pour la zone "raw" du datalake
RAW_DIR = os.path.join(BASE_DIR, "datalake", "raw")


# ==========================================
# Fonction de sauvegarde en format Parquet
# ==========================================

def save_parquet_with_date(df: pd.DataFrame, out_dir: str, base_name: str) -> str:

    # Crée le dossier de sortie s'il n'existe pas
    os.makedirs(out_dir, exist_ok=True)

    # Génère un horodatage au format YYYYMMDD (UTC)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")

    # Construit le chemin complet du fichier Parquet
    path = os.path.join(out_dir, f"{base_name}_{stamp}.parquet")

    # Sauvegarde le DataFrame en Parquet (sans l'index)
    df.to_parquet(path, index=False)

    # Retourne le chemin du fichier créé
    return path


# ==========================================
# Fonction d'ingestion des fichiers CSV
# ==========================================

def ingest_csv(filename: str) -> pd.DataFrame:

    # Construit le chemin complet du fichier CSV
    path = os.path.join(DATA_DIR, filename)

    # Lecture du CSV avec séparateur ';
    return pd.read_csv(
        path,
        sep=";",            # séparateur point-virgule
        encoding="utf-8",   # encodage standard
        low_memory=False    # évite les problèmes de typage
    )


# ==========================================
# Point d'entrée du script
# ==========================================

if __name__ == "__main__":

    # 1) Ingestion des données de trafic (Bordeaux Métropole)
    df_traffic = ingest_csv("ci_trafi_l.csv")
    print("Trafic shape:", df_traffic.shape)
    print("Trafic head:", df_traffic.head())
    print("Trafic infos :", df_traffic.info())
    print("Trafic describe :", df_traffic.describe())
    print("Trafic columns :", df_traffic.columns)
    print("Trafic isnull :", df_traffic.isnull().sum())
  
    # Nettoyage : suppression des colonnes inutiles
    df_traffic.drop(['geo_point_2d', 'geo_shape', 'gml_id', 'gid', 'cdate', 'ident', 'origine', 'code_commune'], axis=1, inplace=True)
    print("Trafic head:", df_traffic.head())
    
    # Sauvegarde des données de trafic en zone raw
    p1 = save_parquet_with_date(df_traffic, RAW_DIR, "trafic_bm_ci_trafi_l")
    print("Saved:", p1)

# ------------------------------------------

    # 2) Ingestion des données de temps de parcours
    df_tps = ingest_csv("ci_tpstj_a.csv")
    print("Temps parcours shape:", df_tps.shape)

    # Sauvegarde des données de temps de parcours en zone raw
    p2 = save_parquet_with_date(df_tps, RAW_DIR, "temps_parcours_bm_ci_tpstj_a")
    print("Saved:", p2)

# ------------------------------------------

