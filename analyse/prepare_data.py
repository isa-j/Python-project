import pandas as pd
import sys
import numpy as np


PATHS = {
    'pop_2020': "/Users/romain/Desktop/Projets DS/Python-project/analyse/data/population_2020.CSV",
    'pop_2014': "/Users/romain/Desktop/Projets DS/Python-project/analyse/data/population_2014.xls",
    'diplo_2020': "/Users/romain/Desktop/Projets DS/Python-project/analyse/data/diplome_2020.CSV",
    'diplo_2014': "/Users/romain/Desktop/Projets DS/Python-project/analyse/data/diplome_2014.xls",
    'rev_2019': "/Users/romain/Desktop/Projets DS/Python-project/analyse/data/revenu_2019.csv",
    'rev_2013': "/Users/romain/Desktop/Projets DS/Python-project/analyse/data/revenu_2013.xls"
}

# df_pop2020 = pd.read_csv(PATHS['pop_2020'], sep=";")
# print(df_pop2020.head())

# df_pop2014 = pd.read_excel(PATHS['pop_2014'])
# print(df_pop2014.head(15))

# df_diplo2020 = pd.read_csv(PATHS['diplo_2020'], sep=";")
# print(df_diplo2020.head())

# df_diplo2014 = pd.read_excel(PATHS['diplo_2014'])
# print(df_diplo2014.head())

# df_revenu2019 = pd.read_csv(PATHS['rev_2019'], sep=";")
# print(df_revenu2019.head(10))

# df_revenu2013 = pd.read_excel(PATHS['rev_2013'])
# print(df_revenu2013.head(10))

# Noms des colonnes (lecture préalable avec .head(10) ou ouverture des fichiers pour vérifier) 
COLS = {
    # Clés de jointure
    'key_iris_com': 'COM',  # Clé commune dans les fichiers IRIS
    'key_com': 'CODGEO',    # Clé commune dans les fichiers Communaux
    'key_final': 'COM',     # Nom de la clé harmonisée finale
    'key_iris_iris': 'IRIS', # Clé IRIS (pour DtypeWarning)
    
    # Population 2020 (IRIS)
    'pop20_pop15p': 'C20_POP15P',
    'pop20_cadres': 'C20_POP15P_CS3',
    'pop20_pop_totale': 'P20_POP',
    
    # Population 2014 (IRIS)
    'pop14_pop15p': 'C14_POP15P',
    'pop14_cadres': 'C14_POP15P_CS3',
    'pop14_pop_totale': 'P14_POP',

    # Diplômes 2020 (IRIS)
    'diplo20_pop15p': 'P20_NSCOL15P',
    'diplo20_sup2': 'P20_NSCOL15P_SUP2',
    'diplo20_sup34': 'P20_NSCOL15P_SUP34',
    'diplo20_sup5': 'P20_NSCOL15P_SUP5',
    
    # Diplômes 2014 (Communal)
    'diplo14_pop15p': 'P14_NSCOL15P',
    'diplo14_sup': 'P14_NSCOL15P_SUP',
    
    # Revenus 2019 (Communal)
    'rev19_med': 'MED19',
    
    # Revenus 2013 (Communal)
    'rev13_med': 'MED13' 
}



def load_population_data(path_2020, path_2014):
    
    # Charge les données de Population (Recensement) 2020 et 2014.
    # Les deux sont au niveau IRIS et doivent être agrégées au niveau Communal.
    
    print("Chargement Population 2020 (IRIS)...")
    try:
        # Spécifier dtype pour les clés résout les DtypeWarning
        df_pop20 = pd.read_csv(
            path_2020, 
            sep=';', 
            dtype={
                COLS['key_iris_com']: str,
                COLS['key_iris_iris']: str
            }
        )
    except FileNotFoundError:
        print(f"ERREUR: Fichier introuvable {path_2020}"); sys.exit()
        
    cols_to_keep_20 = [
        COLS['key_iris_com'], 
        COLS['pop20_pop_totale'],
        COLS['pop20_pop15p'], 
        COLS['pop20_cadres']
    ]
    df_pop20 = df_pop20[cols_to_keep_20]
    
    df_pop20_agg = df_pop20.groupby(COLS['key_iris_com']).sum().reset_index()
    df_pop20_agg['ratio_cadres_20'] = (
        df_pop20_agg[COLS['pop20_cadres']] / df_pop20_agg[COLS['pop20_pop15p']]
    ) * 100
    df_pop20_final = df_pop20_agg.rename(columns={COLS['key_iris_com']: COLS['key_final']})
    df_pop20_final = df_pop20_final[[COLS['key_final'], 'ratio_cadres_20', COLS['pop20_pop_totale']]]
    print(f"-> Pop 2020 agrégée : {df_pop20_final.shape}")


    print("Chargement Population 2014 (IRIS)...")
    try:
        # L'ANALYSE PREALABLE A MONTRE QUE L'en-tête (les COLS) est à l'index 5 (6ème ligne) 
        df_pop14 = pd.read_excel(
            path_2014, 
            header=5, 
            dtype={COLS['key_iris_com']: str}
        )
    except FileNotFoundError:
        print(f"ERREUR: Fichier introuvable {path_2014}"); sys.exit()
    except KeyError as e:
        print(f"ERREUR (KeyError) : Une colonne n'a pas été trouvée à header=5.")
        print(f"Assurez-vous que '{COLS['key_iris_com']}' existe.")
        print(f"Erreur d'origine : {e}")
        sys.exit()
        
    cols_to_keep_14 = [
        COLS['key_iris_com'], 
        COLS['pop14_pop_totale'],
        COLS['pop14_pop15p'], 
        COLS['pop14_cadres']
    ]
    # Vérification que toutes les colonnes existent 
    missing_cols = [col for col in cols_to_keep_14 if col not in df_pop14.columns]
    if missing_cols:
        print(f"ERREUR: Colonnes manquantes dans Pop 2014: {missing_cols}")
        print(f"Colonnes disponibles: {df_pop14.columns.tolist()[:15]}...") # Affiche les 15 premières
        sys.exit()
        
    df_pop14 = df_pop14[cols_to_keep_14]
    
    df_pop14_agg = df_pop14.groupby(COLS['key_iris_com']).sum().reset_index()
    df_pop14_agg['ratio_cadres_14'] = (
        df_pop14_agg[COLS['pop14_cadres']] / df_pop14_agg[COLS['pop14_pop15p']]
    ) * 100
    df_pop14_final = df_pop14_agg.rename(columns={COLS['key_iris_com']: COLS['key_final']})
    df_pop14_final = df_pop14_final[[COLS['key_final'], 'ratio_cadres_14', COLS['pop14_pop_totale']]]
    print(f"-> Pop 2014 agrégée : {df_pop14_final.shape}")
    
    return df_pop20_final, df_pop14_final


def load_diplome_data(path_2020, path_2014):
    
    # Charge les données de Diplômes.
    # 2020 est IRIS (agrégation).
    # 2014 est Communal.
   
    print("Chargement Diplômes 2020 (IRIS)...")
    try:
        df_diplo20 = pd.read_csv(
            path_2020, 
            sep=';', 
            dtype={
                COLS['key_iris_com']: str,
                COLS['key_iris_iris']: str
            }
        )
    except FileNotFoundError:
        print(f"ERREUR: Fichier introuvable {path_2020}"); sys.exit()
        
    cols_sup_20 = [
        COLS['diplo20_sup2'], 
        COLS['diplo20_sup34'], 
        COLS['diplo20_sup5']
    ]
    # S'assurer que les colonnes soient numériques
    for col in cols_sup_20:
        df_diplo20[col] = pd.to_numeric(df_diplo20[col], errors='coerce')

    df_diplo20['diplo_sup_20_agg'] = df_diplo20[cols_sup_20].sum(axis=1)
    
    cols_to_keep_20 = [
        COLS['key_iris_com'], 
        COLS['diplo20_pop15p'], 
        'diplo_sup_20_agg'
    ]
    df_diplo20 = df_diplo20[cols_to_keep_20]
    
    df_diplo20_agg = df_diplo20.groupby(COLS['key_iris_com']).sum().reset_index()
    df_diplo20_agg['ratio_sup_20'] = (
        df_diplo20_agg['diplo_sup_20_agg'] / df_diplo20_agg[COLS['diplo20_pop15p']]
    ) * 100
    df_diplo20_final = df_diplo20_agg.rename(columns={COLS['key_iris_com']: COLS['key_final']})
    df_diplo20_final = df_diplo20_final[[COLS['key_final'], 'ratio_sup_20']]
    print(f"-> Diplo 2020 agrégé : {df_diplo20_final.shape}")


    print("Chargement Diplômes 2014 (Communal)...")
    try:
        # L'ANALYSE PREALABLE A MONTRE QUE L'en-tête (les COLS) est à l'index 5 (6ème ligne) 
        df_diplo14 = pd.read_excel(
            path_2014, 
            header=5, 
            dtype={COLS['key_com']: str}
        )
    except FileNotFoundError:
        print(f"ERREUR: Fichier introuvable {path_2014}"); sys.exit()
        
    df_diplo14['ratio_sup_14'] = (
        df_diplo14[COLS['diplo14_sup']] / df_diplo14[COLS['diplo14_pop15p']]
    ) * 100
    
    df_diplo14_final = df_diplo14.rename(
        columns={COLS['key_com']: COLS['key_final']}
    )
    df_diplo14_final = df_diplo14_final[[COLS['key_final'], 'ratio_sup_14']]
    print(f"-> Diplo 2014 chargé : {df_diplo14_final.shape}")
    
    return df_diplo20_final, df_diplo14_final


def load_revenu_data(path_2019, path_2013):
     
    # Charge les données de Revenus (Filosofi).
    # Les deux sont au niveau Communal.
    
    print("Chargement Revenus 2019 (Communal)...")
    try:
        df_rev19 = pd.read_csv(
            path_2019, 
            sep=';', 
            decimal=',', 
            na_values='s',
            dtype={COLS['key_com']: str}
        )
    except FileNotFoundError:
        print(f"ERREUR: Fichier introuvable {path_2019}"); sys.exit()
        
    df_rev19[COLS['rev19_med']] = pd.to_numeric(
        df_rev19[COLS['rev19_med']], errors='coerce'
    )
    df_rev19_final = df_rev19.rename(
        columns={COLS['key_com']: COLS['key_final']}
    )
    df_rev19_final = df_rev19_final[[COLS['key_final'], COLS['rev19_med']]]
    print(f"-> Rev 2019 chargé : {df_rev19_final.shape}")


    print("Chargement Revenus 2013 (Communal)...")
    try:
        # L'ANALYSE PREALABLE A MONTRE QUE L'en-tête (les COLS) est à l'index 5 (6ème ligne) 
        df_rev13 = pd.read_excel(
            path_2013, 
            header=5, 
            dtype={COLS['key_com']: str}
        )
    except FileNotFoundError:
        print(f"ERREUR: Fichier introuvable {path_2013}"); sys.exit()
        
    # Vérification que la colonne MED13 existe
    if COLS['rev13_med'] not in df_rev13.columns:
        print(f"ERREUR: Colonne '{COLS['rev13_med']}' introuvable dans {path_2013}.")
        print(f"Colonnes disponibles: {df_rev13.columns.tolist()[:15]}...")
        sys.exit()

    df_rev13[COLS['rev13_med']] = pd.to_numeric(
        df_rev13[COLS['rev13_med']], errors='coerce'
    )
    df_rev13_final = df_rev13.rename(
        columns={COLS['key_com']: COLS['key_final']}
    )
    df_rev13_final = df_rev13_final[[COLS['key_final'], COLS['rev13_med']]]
    print(f"-> Rev 2013 chargé : {df_rev13_final.shape}")

    return df_rev19_final, df_rev13_final


#  FONCTION PRINCIPALE (PIPELINE) 

def main():
    print("DÉBUT DU PIPELINE DE FUSION ")
    
    # Étape 1: Charger toutes les briques de données
    df_pop20, df_pop14 = load_population_data(PATHS['pop_2020'], PATHS['pop_2014'])
    df_diplo20, df_diplo14 = load_diplome_data(PATHS['diplo_2020'], PATHS['diplo_2014'])
    df_rev19, df_rev13 = load_revenu_data(PATHS['rev_2019'], PATHS['rev_2013'])
    
    # Étape 2: Harmoniser les clés de jointure
    # Toutes les fonctions retournent maintenant un DataFrame
    # avec la clé harmonisée COLS['key_final'] ('COM')
    
    data_frames = [
        df_pop14,
        df_diplo20,
        df_diplo14,
        df_rev19,
        df_rev13
    ]
    
    # Étape 3: Fusionner en série
    print("\nDÉBUT DES FUSIONS ")
    
    master_df = df_pop20
    print(f"Base de départ (Pop 2020): {master_df.shape[0]} communes")
    
    for i, df_to_merge in enumerate(data_frames, 1):
        # Vérification des colonnes avant fusion
        if COLS['key_final'] not in df_to_merge.columns:
            print(f"ERREUR: Clé '{COLS['key_final']}' manquante dans le df #{i}.")
            sys.exit()

        master_df = pd.merge(
            master_df, 
            df_to_merge, 
            on=COLS['key_final'], 
            how='inner'
        )
        print(f"  -> Après fusion #{i}... {master_df.shape[0]} communes restantes")
        
    if master_df.empty:
        print("ERREUR FATALE: La fusion a produit un DataFrame vide.")
        print("Vérifiez les clés de jointure ('COM') et les fichiers.")
        sys.exit()
        
    print("FUSIONS TERMINÉES")

    # Étape 4: Sauvegarde du fichier fusionné
    output_path = 'master_data_fusionne.csv'
    master_df.to_csv(output_path, index=False, sep=';', decimal=',')
    
    print(f"\n--- PIPELINE TERMINÉ ---")
    print(f"Fichier fusionné '{output_path}' créé avec succès.")
    print(f"Taille finale: {master_df.shape[0]} lignes (communes), {master_df.shape[1]} colonnes")
    print("\nAperçu du master_df:")
    print(master_df.head())


#  EXÉCUTION DU SCRIPT 
if __name__ == "__main__":
    main()