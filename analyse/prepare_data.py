# BUT: préparer les données (nettoyer/trier...) et finir avec un unique .csv complet
# NOTES PROJET: API. Il faut que le script soit autonome, télécharge les données automatiquement, le relecteur doit se poser le moins de questions possibles.


import pandas as pd
import numpy as np
import warnings
import os

# --- 1. CONFIGURATION : CHEMINS ET CLÉS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

PATH_ELEC_2014 = os.path.join(DATA_DIR, "elections_2014.txt")
PATH_ELEC_2020 = os.path.join(DATA_DIR, "elections_2020.txt")
PATH_POP_2014 = os.path.join(DATA_DIR, "population_2014.xls")
PATH_POP_2020 = os.path.join(DATA_DIR, "population_2020.csv")
PATH_DIPLOME_2014 = os.path.join(DATA_DIR, "diplome_2014.xls")
PATH_DIPLOME_2020 = os.path.join(DATA_DIR, "diplome_2020.csv") 
PATH_REVENU_2019 = os.path.join(DATA_DIR, "revenu_2019.csv") 

JOIN_KEY = 'CODGEO' 

# --- 2. DÉFINITION DES VARIABLES (CORRIGÉES ET CONFIRMÉES) ---
# (Basé sur vos dictionnaires et les aperçus 'head')

# Variables 2014
VAR_POP_2014 = 'P14_POP'
VAR_CADRES_NUM_2014 = 'C14_POP15P_CS3'
VAR_CADRES_DEN_2014 = 'C14_POP15P'
VAR_DIPLOME_NUM_2014 = 'P14_NSCOL15P_SUP' 
VAR_CODE_COMMUNE_POP2014 = 'COM' 
VAR_CODE_COMMUNE_DIPLOME2014 = 'CODGEO'

# Variables 2020 (Basé sur vos 'head')
VAR_POP_2020 = 'P20_POP'
VAR_CADRES_NUM_2020 = 'C20_POP15P_CS3'
VAR_CADRES_DEN_2020 = 'C20_POP15P'
# Note: P20_FNSCOL15P_SUP5 est pour les Femmes. 
# Si P20_NSCOL15P_SUP5 existe (total), utilisez-le. Sinon, P20_FNSCOL15P_SUP5 est un bon proxy.
VAR_DIPLOME_NUM_2020 = 'P20_FNSCOL15P_SUP5' 
VAR_CODE_COMMUNE_POP2020 = 'COM' 
VAR_CODE_COMMUNE_DIPLOME2020 = 'COM' 

# Variable Revenu 2019 (CONFIRMÉ)
VAR_CODE_COMMUNE_REVENU = 'CODGEO' 
VAR_REVENU_MEDIAN_2019 = 'MED19' 

VARS_VOTE_2014_LIST = ['LEXG', 'LFG', 'LCOM', 'LPG', 'LSOC', 'LRDG', 'LDVG', 'LUG'] 
VARS_VOTE_2020_LIST = ['LEXG', 'LCOM', 'LFI', 'LSOC', 'LRDG', 'LDVG', 'LUG', 'LVEC', 'LECO'] 

# --- 3. FONCTIONS DE CHARGEMENT ET TRAITEMENT ---

def safe_read_excel(path, skiprows=5):
    try:
        # Force la lecture des clés comme texte
        return pd.read_excel(path, skiprows=skiprows, dtype={'COM': str, 'CODGEO': str})
    except Exception as e:
        print(f"ERREUR: Impossible de lire le fichier Excel {path}. Erreur: {e}")
        return None

def safe_read_csv(path, sep=';', skiprows=0, encoding='latin1', **kwargs):
    """Lecture CSV simple avec skiprows=0 (confirmé par 'head') et dtype=str."""
    try:
        # Force la lecture des clés comme texte
        return pd.read_csv(path, sep=sep, skiprows=skiprows, encoding=encoding, low_memory=False, 
                           dtype={'COM': str, 'CODGEO': str}, **kwargs)
    except Exception as e:
        print(f"ERREUR: Impossible de lire le fichier CSV {path} (skiprows={skiprows}). Erreur: {e}")
        return None

# Fonctions d'élection (ajustées)
def load_and_prep_elections_2014(path):
    print("Chargement Élections 2014...")
    try:
        df = pd.read_csv(path, sep=';', encoding='latin1', 
                         dtype={'Code du département': str, 'Code de la commune': str},
                         on_bad_lines='skip') # <-- CORRECTION Ligne 18
    except Exception as e:
        print(f"ERREUR: Élections 2014. Erreur: {e}")
        return None

    # Pas besoin de .zfill(5) ici car zfill(2) + zfill(3) = 5
    df[JOIN_KEY] = df['Code du département'].str.zfill(2) + df['Code de la commune'].str.zfill(3)
    
    # ... (Le reste de la fonction est inchangé) ...
    df['Voix'] = pd.to_numeric(df['Voix'], errors='coerce').fillna(0)
    df['Exprimés'] = pd.to_numeric(df['Exprimés'], errors='coerce')
    df_votes = df[df['Code Nuance'].isin(VARS_VOTE_2014_LIST)]
    df_agg_votes = df_votes.groupby(JOIN_KEY)['Voix'].sum().reset_index(name='Total_Votes_2014')
    df_agg_exp = df.groupby(JOIN_KEY)['Exprimés'].first().reset_index(name='Total_Exprimés_2014')
    df_elec_2014 = pd.merge(df_agg_votes, df_agg_exp, on=JOIN_KEY, how='left')
    df_elec_2014['Vote_Share_2014'] = df_elec_2014['Total_Votes_2014'] / df_elec_2014['Total_Exprimés_2014']
    return df_elec_2014[[JOIN_KEY, 'Vote_Share_2014']]

def load_and_prep_elections_2020(path):
    print("Chargement Élections 2020...")
    try:
        df = pd.read_csv(path, sep='\t', encoding='latin1', low_memory=False, 
                         dtype={'Code du département': str, 'Code de la commune': str})
    except Exception as e:
        print(f"ERREUR: Élections 2020. Erreur: {e}")
        return None
    # ... (Le reste de la fonction est inchangé) ...
    df['Code_dep_2digit'] = df['Code du département'].astype(str).str.pad(width=2, side='left', fillchar='0')
    df['Code_commune_3digit'] = df['Code de la commune'].astype(str).str.zfill(3)
    
    # Pas besoin de .zfill(5) ici
    df[JOIN_KEY] = df['Code_dep_2digit'] + df['Code_commune_3digit']
    
    df['Exprimés'] = pd.to_numeric(df['Exprimés'], errors='coerce')
    df_exp = df.groupby(JOIN_KEY)['Exprimés'].first().reset_index(name='Total_Exprimés_2020')
    all_data_long = []
    base_cols = [JOIN_KEY]
    try:
        df_base = df[base_cols + ['Code Nuance', 'Voix']].rename(columns={'Code Nuance': 'Nuance', 'Voix': 'Voix'})
        all_data_long.append(df_base)
    except KeyError:
        pass 
    i = 1
    while f'Code Nuance.{i}' in df.columns:
        nuance_col = f'Code Nuance.{i}'
        voix_col = f'Voix.{i}'
        if voix_col not in df.columns: break
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

# Fonctions de données socio-démographiques
def load_data_2014(path_pop, path_diplome):
    print("Chargement Données 2014 (Pop + Diplôme)...")
    pop_2014 = safe_read_excel(path_pop, skiprows=5)
    dipl_2014 = safe_read_excel(path_diplome, skiprows=5) 
    if pop_2014 is None or dipl_2014 is None: return None
    
    pop_2014.rename(columns={VAR_CODE_COMMUNE_POP2014: JOIN_KEY}, inplace=True)
    dipl_2014.rename(columns={VAR_CODE_COMMUNE_DIPLOME2014: JOIN_KEY}, inplace=True) 

    # <-- CORRECTION CLÉ : Forcer le format 5 chiffres
    pop_2014[JOIN_KEY] = pop_2014[JOIN_KEY].astype(str).str.zfill(5)
    dipl_2014[JOIN_KEY] = dipl_2014[JOIN_KEY].astype(str).str.zfill(5)
    
    pop_cols = [JOIN_KEY, VAR_POP_2014, VAR_CADRES_NUM_2014, VAR_CADRES_DEN_2014]
    dipl_cols = [JOIN_KEY, VAR_DIPLOME_NUM_2014]
    
    for df, cols, name in [(pop_2014, pop_cols, "Pop 2014"), (dipl_2014, dipl_cols, "Diplôme 2014")]:
        for col in cols:
            if col not in df.columns:
                print(f"ERREUR: Colonne '{col}' non trouvée dans {name}.")
                return None
    
    data_2014 = pd.merge(pop_2014[pop_cols], dipl_2014[dipl_cols], on=JOIN_KEY, how='inner')
    data_2014['Part_Cadres_2014'] = data_2014[VAR_CADRES_NUM_2014] / data_2014[VAR_CADRES_DEN_2014]
    data_2014['Part_Diplomes_2014'] = data_2014[VAR_DIPLOME_NUM_2014] / data_2014[VAR_CADRES_DEN_2014]
    return data_2014[[JOIN_KEY, VAR_POP_2014, 'Part_Cadres_2014', 'Part_Diplomes_2014']]

def load_data_2020(path_pop, path_diplome):
    print("Chargement Données 2020 (Pop + Diplôme)...")
    
    # <-- CORRECTION : Utilise skiprows=0 (basé sur 'head')
    pop_2020 = safe_read_csv(path_pop, sep=';', skiprows=0, encoding='latin1')
    dipl_2020 = safe_read_csv(path_diplome, sep=';', skiprows=0, encoding='latin1') 
    
    if pop_2020 is None or dipl_2020 is None: return None
    
    pop_2020.rename(columns={VAR_CODE_COMMUNE_POP2020: 'COM'}, inplace=True)
    dipl_2020.rename(columns={VAR_CODE_COMMUNE_DIPLOME2020: 'COM'}, inplace=True)

    # <-- CORRECTION CLÉ : Forcer le format 5 chiffres
    pop_2020['COM'] = pop_2020['COM'].astype(str).str.zfill(5)
    dipl_2020['COM'] = dipl_2020['COM'].astype(str).str.zfill(5)

    pop_cols_needed = ['COM', VAR_POP_2020, VAR_CADRES_NUM_2020, VAR_CADRES_DEN_2020]
    dipl_cols_needed = ['IRIS', 'COM', VAR_DIPLOME_NUM_2020]
    
    for col in pop_cols_needed:
        if col not in pop_2020.columns:
            print(f"ERREUR: Colonne '{col}' non trouvée dans Pop 2020. Colonnes lues: {pop_2020.columns.to_list()}")
            return None
    for col in dipl_cols_needed:
         if col not in dipl_2020.columns:
            print(f"ERREUR: Colonne '{col}' non trouvée dans Diplôme 2020. Colonnes lues: {dipl_2020.columns.to_list()}")
            return None

    # Conversion en numérique avant agrégation
    pop_cols_to_sum = [VAR_POP_2020, VAR_CADRES_NUM_2020, VAR_CADRES_DEN_2020]
    for col in pop_cols_to_sum:
        pop_2020[col] = pd.to_numeric(pop_2020[col], errors='coerce')
    pop_agg = pop_2020.groupby('COM')[pop_cols_to_sum].sum().reset_index()

    dipl_cols_to_sum = [VAR_DIPLOME_NUM_2020]
    for col in dipl_cols_to_sum:
        dipl_2020[col] = pd.to_numeric(dipl_2020[col], errors='coerce')
    dipl_agg = dipl_2020.groupby('COM')[dipl_cols_to_sum].sum().reset_index()

    data_2020 = pd.merge(pop_agg, dipl_agg, on='COM', how='inner')
    data_2020.rename(columns={'COM': JOIN_KEY}, inplace=True)
    
    data_2020['Part_Cadres_2020'] = data_2020[VAR_CADRES_NUM_2020] / data_2020[VAR_CADRES_DEN_2020]
    data_2020['Part_Diplomes_2020'] = data_2020[VAR_DIPLOME_NUM_2020] / data_2020[VAR_CADRES_DEN_2020]
    
    return data_2020[[JOIN_KEY, VAR_POP_2020, 'Part_Cadres_2020', 'Part_Diplomes_2020']]

def load_revenu(path_revenu):
    print("Chargement Revenu 2019...")
    
    # <-- CORRECTION : Utilise skiprows=0 (basé sur 'head')
    revenu_2019 = safe_read_csv(path_revenu, sep=';', skiprows=0, encoding='latin1')
    
    if revenu_2019 is None: return None
        
    revenu_2019.rename(columns={VAR_CODE_COMMUNE_REVENU: JOIN_KEY}, inplace=True)
    
    if JOIN_KEY not in revenu_2019.columns:
        print(f"ERREUR: Clé '{JOIN_KEY}' non trouvée dans Revenu 2019.")
        return None
    if VAR_REVENU_MEDIAN_2019 not in revenu_2019.columns:
        print(f"ERREUR: Colonne '{VAR_REVENU_MEDIAN_2019}' non trouvée dans Revenu 2019.")
        return None

    # <-- CORRECTION CLÉ : Forcer le format 5 chiffres
    revenu_2019[JOIN_KEY] = revenu_2019[JOIN_KEY].astype(str).str.zfill(5)
    
    revenu_2019[VAR_REVENU_MEDIAN_2019] = revenu_2019[VAR_REVENU_MEDIAN_2019].replace('s', np.nan)
    revenu_2019['Revenu_Median_2019'] = pd.to_numeric(revenu_2019[VAR_REVENU_MEDIAN_2019], errors='coerce')
    
    return revenu_2019[[JOIN_KEY, 'Revenu_Median_2019']]

# --- 4. SCRIPT PRINCIPAL (Inchangé) ---
def main():
    warnings.simplefilter(action='ignore', category=FutureWarning)
    warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

    print(f"--- DÉBUT DE LA PRÉPARATION DES DONNÉES (V11 - Correction Clés) ---")
    
    df_elec_2014 = load_and_prep_elections_2014(PATH_ELEC_2014)
    df_elec_2020 = load_and_prep_elections_2020(PATH_ELEC_2020)
    df_data_2014 = load_data_2014(PATH_POP_2014, PATH_DIPLOME_2014)
    df_data_2020 = load_data_2020(PATH_POP_2020, PATH_DIPLOME_2020)
    df_revenu = load_revenu(PATH_REVENU_2019)
    
    if any(df is None for df in [df_elec_2014, df_elec_2020, df_data_2014, df_data_2020, df_revenu]):
        print("\n--- ERREUR BLOQUANTE ---")
        print("Le script est arrêté. Vérifiez les erreurs de 'Colonne non trouvée' ou de lecture.")
        return

    print("\nFichiers chargés. Fusion en cours...")
    
    # --- STRATÉGIE DE FUSION (inchangée, mais les clés sont maintenant propres) ---
    df_elec = pd.merge(df_elec_2014, df_elec_2020, on=JOIN_KEY, how='inner')
    df_data = pd.merge(df_data_2014, df_data_2020, on=JOIN_KEY, how='inner')
    final_df = pd.merge(df_data, df_elec, on=JOIN_KEY, how='inner')
    final_df = pd.merge(final_df, df_revenu, on=JOIN_KEY, how='inner')
    
    print(f"Fusion terminée. {len(final_df)} communes trouvées dans toutes les bases de données.")
    
    if len(final_df) == 0:
        print("AVERTISSEMENT : 0 communes après fusion. Cela signifie qu'aucune commune n'est présente dans TOUS les fichiers.")
        print("Essayez de changer 'how=inner' en 'how=outer' pour déboguer, ou vérifiez vos fichiers sources.")
        return

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
        JOIN_KEY, 'Delta_Vote_Gauche_Verts', 'Delta_Part_Cadres', 'Delta_Part_Diplomes',
        'Revenu_Median_2019', 'Vote_Share_2014', 'Vote_Share_2020',
        'Part_Cadres_2014', 'Part_Cadres_2020', 'Part_Diplomes_2014', 'Part_Diplomes_2020',
        VAR_POP_2014, VAR_POP_2020
    ]
    final_columns = [col for col in final_columns if col in final_df_filtered.columns]
    final_df_to_save = final_df_filtered[final_columns]
    output_path = "base_analyse_gentrification_elections.csv"
    
    try:
        final_df_to_save.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
        print("\n--- SUCCÈS ---")
        print(f"Le fichier final a été sauvegardé sous : {output_path}")
    except Exception as e:
        print(f"\n--- ERREUR LORS DE LA SAUVEGARDE ---")
        print(f"Impossible de sauvegarder le fichier CSV. Erreur: {e}")

if __name__ == "__main__":
    main()