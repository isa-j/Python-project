# BUT: préparer les données (nettoyer/trier...) et finir avec un unique .csv complet

import pandas as pd
import numpy as np
import warnings
import os

# --- 1. CONFIGURATION : MODIFIEZ CES CHEMINS ---
# (J'ai mis à jour les noms pour correspondre à vos messages d'erreur)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Fichiers Élections
PATH_ELEC_2014 = os.path.join(DATA_DIR, "elections_2014.txt")
PATH_ELEC_2020 = os.path.join(DATA_DIR, "elections_2020.txt") 

# Fichiers de données INSEE
PATH_POP_2014 = os.path.join(DATA_DIR, "population_2014.xls")
PATH_POP_2020 = os.path.join(DATA_DIR, "population_2020.csv")
PATH_DIPLOME_2014 = os.path.join(DATA_DIR, "diplome_2014.xls")
PATH_DIPLOME_2020 = os.path.join(DATA_DIR, "diplome_2020.csv") # <-- MODIFIÉ (correspond à votre erreur)
PATH_REVENU_2019 = os.path.join(DATA_DIR, "revenu_2019.csv") # <-- MODIFIÉ (correspond à votre erreur)

# Clé de jointure principale (Code Commune INSEE)
JOIN_KEY = 'COM'

# --- 2. DÉFINITION DES VARIABLES ET NUANCES ---
# (Nous devrons peut-être ajuster ces noms après le prochain test)

# Variables Population & Cadres
VAR_POP_2014 = 'P14_POP'
VAR_CADRES_NUM_2014 = 'C14_POP15P_CS3'
VAR_CADRES_DEN_2014 = 'C14_POP15P'
VAR_POP_2020 = 'P20_POP'
VAR_CADRES_NUM_2020 = 'C20_POP15P_CS3'
VAR_CADRES_DEN_2020 = 'C20_POP15P'

# Variables Diplôme
VAR_DIPLOME_NUM_2014 = 'P09_NSCOL15P_SUP' # <-- À VÉRIFIER
VAR_DIPLOME_DEN_2014 = VAR_CADRES_DEN_2014 
VAR_DIPLOME_NUM_2020 = 'P20_NSCOL15P_SUP5' # <-- À VÉRIFIER
VAR_DIPLOME_DEN_2020 = VAR_CADRES_DEN_2020 

# Variable Revenu
VAR_REVENU_MEDIAN_2019 = 'MED19' # <-- À VÉRIFIER
VAR_CODE_COMMUNE_REVENU = 'CODGEO' # <-- À VÉRIFIER

# Listes de nuances politiques pour l'agrégation
VARS_VOTE_2014_LIST = [
    'LEXG', 'LFG', 'LCOM', 'LPG', 'LSOC', 'LRDG', 'LDVG', 'LUG'
] 
VARS_VOTE_2020_LIST = [
    'LEXG', 'LCOM', 'LFI', 'LSOC', 'LRDG', 'LDVG', 'LUG', 'LVEC', 'LECO'
] 

# --- 3. FONCTIONS DE CHARGEMENT ET TRAITEMENT ---

def safe_read_excel(path, skiprows=5):
    """Charge un fichier Excel INSEE en ignorant les 5 premières lignes."""
    try:
        return pd.read_excel(path, skiprows=skiprows)
    except Exception as e:
        print(f"ERREUR: Impossible de lire le fichier Excel {path}. Vérifiez le chemin et le format. Erreur: {e}")
        return None

def safe_read_csv(path, sep=';', skiprows=5, encoding='latin1'):
    """Charge un fichier CSV INSEE en ignorant les 5 premières lignes."""
    try:
        return pd.read_csv(path, sep=sep, skiprows=skiprows, encoding=encoding, low_memory=False)
    except Exception as e:
        print(f"ERREUR: Impossible de lire le fichier CSV {path}. (sep='{sep}', encoding='{encoding}'). Erreur: {e}")
        return None

def load_and_prep_elections_2014(path):
    """Charge et nettoie le fichier électoral 2014 (mal formaté)."""
    print("Chargement Élections 2014...")
    try:
        df = pd.read_csv(path, sep=';', encoding='latin1', low_memory=False,
                         dtype={'Code du département': str, 'Code de la commune': str},
                         engine='python') # <-- MODIFIÉ (Solution Erreur 1)
    except Exception as e:
        print(f"ERREUR: Impossible de lire {path}. Assurez-vous qu'il est au format 'txt' avec séparateur ';'. Erreur: {e}")
        return None

    # ... (le reste de la fonction est inchangé) ...
    df[JOIN_KEY] = df['Code du département'].str.zfill(2) + df['Code de la commune'].str.zfill(3)
    df['Voix'] = pd.to_numeric(df['Voix'], errors='coerce').fillna(0)
    df['Exprimés'] = pd.to_numeric(df['Exprimés'], errors='coerce')
    df_votes = df[df['Code Nuance'].isin(VARS_VOTE_2014_LIST)]
    df_agg_votes = df_votes.groupby(JOIN_KEY)['Voix'].sum().reset_index(name='Total_Votes_2014')
    df_agg_exp = df.groupby(JOIN_KEY)['Exprimés'].first().reset_index(name='Total_Exprimés_2014')
    df_elec_2014 = pd.merge(df_agg_votes, df_agg_exp, on=JOIN_KEY, how='left')
    df_elec_2014['Vote_Share_2014'] = df_elec_2014['Total_Votes_2014'] / df_elec_2014['Total_Exprimés_2014']
    
    return df_elec_2014[[JOIN_KEY, 'Vote_Share_2014']]

def load_and_prep_elections_2020(path):
    """Charge et nettoie le fichier électoral 2020 (format large)."""
    print("Chargement Élections 2020...")
    try:
        df = pd.read_csv(path, sep='\t', encoding='latin1', low_memory=False, # <-- MODIFIÉ (Solution Erreur 2)
                         dtype={'Code du département': str, 'Code de la commune': str})
    except Exception as e:
        print(f"ERREUR: Impossible de lire {path}. Assurez-vous qu'il est au format 'txt' avec séparateur TAB. Erreur: {e}")
        return None

    # ... (le reste de la fonction est inchangé) ...
    df.rename(columns={
        'Code du département': 'Code_dep_2digit',
        'Code de la commune': 'Code_commune_3digit'
    }, inplace=True)
    df['Code_dep_2digit'] = df['Code_dep_2digit'].astype(str).str.pad(width=2, side='left', fillchar='0')
    df['Code_commune_3digit'] = df['Code_commune_3digit'].astype(str).str.zfill(3)
    df[JOIN_KEY] = df['Code_dep_2digit'] + df['Code_commune_3digit']
    df['Exprimés'] = pd.to_numeric(df['Exprimés'], errors='coerce')
    df_exp = df.groupby(JOIN_KEY)['Exprimés'].first().reset_index(name='Total_Exprimés_2020')
    all_data_long = []
    base_cols = [JOIN_KEY]
    try:
        df_base = df[base_cols + ['Code Nuance', 'Voix']].rename(columns={'Code Nuance': 'Nuance', 'Voix': 'Voix'})
        all_data_long.append(df_base)
    except KeyError:
        print("Avertissement: Colonnes 'Code Nuance' ou 'Voix' de base non trouvées.")
    i = 1
    while f'Code Nuance.{i}' in df.columns:
        nuance_col = f'Code Nuance.{i}'
        voix_col = f'Voix.{i}'
        if voix_col not in df.columns:
            break
        df_part = df[base_cols + [nuance_col, voix_col]].rename(columns={nuance_col: 'Nuance', voix_col: 'Voix'})
        all_data_long.append(df_part)
        i += 1
    if not all_data_long:
        print("ERREUR: Impossible de trouver des colonnes de nuances/voix dans le fichier 2020.")
        return None
    df_long = pd.concat(all_data_long, ignore_index=True)
    df_long.dropna(subset=['Nuance', 'Voix'], inplace=True)
    df_long['Voix'] = pd.to_numeric(df_long['Voix'], errors='coerce').fillna(0)
    df_votes = df_long[df_long['Nuance'].isin(VARS_VOTE_2020_LIST)]
    df_agg_votes = df_votes.groupby(JOIN_KEY)['Voix'].sum().reset_index(name='Total_Votes_2020')
    df_elec_2020 = pd.merge(df_agg_votes, df_exp, on=JOIN_KEY, how='left')
    df_elec_2020['Vote_Share_2020'] = df_elec_2020['Total_Votes_2020'] / df_elec_2020['Total_Exprimés_2020']
    
    return df_elec_2020[[JOIN_KEY, 'Vote_Share_2020']]

def load_data_2014(path_pop, path_diplome):
    """Charge et fusionne les données de Population et Diplôme 2014."""
    print("Chargement Données 2014 (Pop + Diplôme)...")
    pop_2014 = safe_read_excel(path_pop, skiprows=5)
    dipl_2014 = safe_read_excel(path_diplome, skiprows=5)
    
    if pop_2014 is None or dipl_2014 is None:
        return None

    # <-- MODIFIÉ (Solution Erreur 4 - Debug)
    print("--- DEBUG DIPLOME 2014 ---")
    print(f"Colonnes trouvées dans {path_diplome}: \n{dipl_2014.columns.to_list()}\n")
    print("--- FIN DEBUG ---")

    pop_2014.rename(columns={'COM': JOIN_KEY}, inplace=True)
    dipl_2014.rename(columns={'CODGEO': JOIN_KEY}, inplace=True)
    pop_2014[JOIN_KEY] = pop_2014[JOIN_KEY].astype(str)
    dipl_2014[JOIN_KEY] = dipl_2014[JOIN_KEY].astype(str)
    pop_cols = [JOIN_KEY, VAR_POP_2014, VAR_CADRES_NUM_2014, VAR_CADRES_DEN_2014]
    dipl_cols = [JOIN_KEY, VAR_DIPLOME_NUM_2014]
    for df, cols, name in [(pop_2014, pop_cols, "Pop 2014"), (dipl_2014, dipl_cols, "Diplôme 2014")]:
        for col in cols:
            if col not in df.columns:
                print(f"ERREUR: Colonne '{col}' non trouvée dans {name}. Vérifiez la section 'DÉFINITION DES VARIABLES'.")
                return None
    
    pop_2014 = pop_2014[pop_cols]
    dipl_2014 = dipl_2014[dipl_cols]
    data_2014 = pd.merge(pop_2014, dipl_2014, on=JOIN_KEY, how='inner')
    data_2014['Part_Cadres_2014'] = data_2014[VAR_CADRES_NUM_2014] / data_2014[VAR_CADRES_DEN_2014]
    data_2014['Part_Diplomes_2014'] = data_2014[VAR_DIPLOME_NUM_2014] / data_2014[VAR_CADRES_DEN_2014]
    
    return data_2014[[JOIN_KEY, VAR_POP_2014, 'Part_Cadres_2014', 'Part_Diplomes_2014']]

def load_data_2020(path_pop, path_diplome):
    """Charge et fusionne les données de Population et Diplôme 2020."""
    print("Chargement Données 2020 (Pop + Diplôme)...")
    
    pop_2020 = safe_read_csv(path_pop, sep=';', skiprows=5, encoding='latin1')
    # <-- MODIFIÉ (Solution Erreur 3)
    dipl_2020 = safe_read_csv(path_diplome, sep=';', skiprows=5, encoding='latin1') 
    
    if pop_2020 is None or dipl_2020 is None:
        return None

    # <-- MODIFIÉ (Debug)
    print("--- DEBUG DIPLOME 2020 ---")
    print(f"Colonnes trouvées dans {path_diplome}: \n{dipl_2020.columns.to_list()}\n")
    print("--- FIN DEBUG ---")

    pop_2020.rename(columns={'COM': JOIN_KEY}, inplace=True)
    dipl_2020.rename(columns={'CODGEO': JOIN_KEY}, inplace=True)
    pop_2020[JOIN_KEY] = pop_2020[JOIN_KEY].astype(str)
    dipl_2020[JOIN_KEY] = dipl_2020[JOIN_KEY].astype(str)
    pop_cols = [JOIN_KEY, VAR_POP_2020, VAR_CADRES_NUM_2020, VAR_CADRES_DEN_2020]
    dipl_cols = [JOIN_KEY, VAR_DIPLOME_NUM_2020]
    for df, cols, name in [(pop_2020, pop_cols, "Pop 2020"), (dipl_2020, dipl_cols, "Diplôme 2020")]:
        for col in cols:
            if col not in df.columns:
                print(f"ERREUR: Colonne '{col}' non trouvée dans {name}. Vérifiez la section 'DÉFINITION DES VARIABLES'.")
                return None
                
    pop_2020 = pop_2020[pop_cols]
    dipl_2020 = dipl_2020[dipl_cols]
    data_2020 = pd.merge(pop_2020, dipl_2020, on=JOIN_KEY, how='inner')
    data_2020['Part_Cadres_2020'] = data_2020[VAR_CADRES_NUM_2020] / data_2020[VAR_CADRES_DEN_2020]
    data_2020['Part_Diplomes_2020'] = data_2020[VAR_DIPLOME_NUM_2020] / data_2020[VAR_CADRES_DEN_2020]
    
    return data_2020[[JOIN_KEY, VAR_POP_2020, 'Part_Cadres_2020', 'Part_Diplomes_2020']]

def load_revenu(path_revenu):
    """Charge le revenu médian 2019."""
    print("Chargement Revenu 2019...")
    revenu_2019 = safe_read_csv(path_revenu, sep=';', skiprows=5, encoding='latin1')
    
    if revenu_2019 is None:
        return None

    # <-- MODIFIÉ (Solution Erreur 5 - Debug)
    print("--- DEBUG REVENU 2019 ---")
    print(f"Colonnes trouvées dans {path_revenu}: \n{revenu_2019.columns.to_list()}\n")
    print("--- FIN DEBUG ---")
        
    if VAR_CODE_COMMUNE_REVENU not in revenu_2019.columns or VAR_REVENU_MEDIAN_2019 not in revenu_2019.columns:
        print(f"ERREUR: Colonnes '{VAR_CODE_COMMUNE_REVENU}' ou '{VAR_REVENU_MEDIAN_2019}' non trouvées dans {path_revenu}. Vérifiez la section 'DÉFINITION DES VARIABLES'.")
        return None

    revenu_2019.rename(columns={VAR_CODE_COMMUNE_REVENU: JOIN_KEY}, inplace=True)
    revenu_2019[JOIN_KEY] = revenu_2019[JOIN_KEY].astype(str)
    revenu_2019['Revenu_Median_2019'] = pd.to_numeric(revenu_2019[VAR_REVENU_MEDIAN_2019], errors='coerce')
    
    return revenu_2019[[JOIN_KEY, 'Revenu_Median_2019']]

# --- 4. SCRIPT PRINCIPAL ---

def main():
    warnings.simplefilter(action='ignore', category=FutureWarning)
    warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

    print("--- DÉBUT DE LA PRÉPARATION DES DONNÉES (V3) ---")
    
    # Charger toutes les sources de données
    df_elec_2014 = load_and_prep_elections_2014(PATH_ELEC_2014)
    df_elec_2020 = load_and_prep_elections_2020(PATH_ELEC_2020)
    df_data_2014 = load_data_2014(PATH_POP_2014, PATH_DIPLOME_2014)
    df_data_2020 = load_data_2020(PATH_POP_2020, PATH_DIPLOME_2020)
    df_revenu = load_revenu(PATH_REVENU_2019)
    
    if any(df is None for df in [df_elec_2014, df_elec_2020, df_data_2014, df_data_2020, df_revenu]):
        print("\n--- ERREUR ---")
        print("Un ou plusieurs fichiers n'ont pas pu être chargés. Veuillez vérifier les messages d'erreur ci-dessus.")
        print("Si l'erreur est 'Colonne ... non trouvée', lisez les listes de colonnes 'DEBUG' pour trouver le bon nom.")
        print("Le script est arrêté.")
        return

    print("\nFichiers chargés. Fusion en cours...")
    
    # ... (le reste du script est inchangé) ...
    df_elec = pd.merge(df_elec_2014, df_elec_2020, on=JOIN_KEY, how='inner')
    df_data = pd.merge(df_data_2014, df_data_2020, on=JOIN_KEY, how='inner')
    final_df = pd.merge(df_data, df_elec, on=JOIN_KEY, how='inner')
    final_df = pd.merge(final_df, df_revenu, on=JOIN_KEY, how='inner')
    
    print(f"Fusion terminée. {len(final_df)} communes trouvées dans toutes les bases de données.")
    
    print("Calcul des variables (Deltas)...")
    final_df['Delta_Part_Cadres'] = final_df['Part_Cadres_2020'] - final_df['Part_Cadres_2014']
    final_df['Delta_Part_Diplomes'] = final_df['Part_Diplomes_2020'] - final_df['Part_Diplomes_2014']
    final_df['Delta_Vote_Gauche_Verts'] = final_df['Vote_Share_2020'] - final_df['Vote_Share_2014']
    final_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    pop_filter = 3500
    final_df_filtered = final_df[final_df[VAR_POP_2020] > pop_filter].copy()
    
    print(f"{len(final_df) - len(final_df_filtered)} communes filtrées (pop <= {pop_filter}).")
    print(f"{len(final_df_filtered)} communes restantes pour l'analyse.")
    
    final_columns = [
        JOIN_KEY,
        'Delta_Vote_Gauche_Verts',  # Y
        'Delta_Part_Cadres',        # X1
        'Delta_Part_Diplomes',      # X2
        'Revenu_Median_2019',       # X3 (Stock)
        'Vote_Share_2014', 'Vote_Share_2020',
        'Part_Cadres_2014', 'Part_Cadres_2020',
        'Part_Diplomes_2014', 'Part_Diplomes_2020',
        VAR_POP_2014, VAR_POP_2020
    ]
    final_columns = [col for col in final_columns if col in final_df_filtered.columns]
    final_df_to_save = final_df_filtered[final_columns]
    
    output_path = "base_analyse_gentrification_elections.csv"
    
    try:
        final_df_to_save.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
        print("\n--- SUCCÈS ---")
        print(f"Le fichier final a été sauvegardé sous : {output_path}")
        print("\nAperçu des données finales :")
        print(final_df_to_save.head())
        print("\nDescription des variables (NaNs, etc.) :")
        print(final_df_to_save.describe(include='all'))
        
    except Exception as e:
        print(f"\n--- ERREUR LORS DE LA SAUVEGARDE ---")
        print(f"Impossible de sauvegarder le fichier CSV. Erreur: {e}")

if __name__ == "__main__":
    main()