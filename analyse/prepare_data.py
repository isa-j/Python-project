# BUT: préparer les données (nettoyer/trier...) et finir avec un unique .csv complet

import pandas as pd
import warnings
import sys # Importé pour pouvoir arrêter le script en cas d'erreur fatale

# A FAIRE: 1) envoyer les données dans dossier data/ dans le local git (gitignore va les ignorer et ne pas les envoyer sur le repo Git)
# 2) ouvrir chaque .csv, vérifier les noms des colonnes dans les vrais fichiers data
# 3) remplir le dictionnaire COLS avec les noms de colonnes corrects
# 4) vérifier les PATHS

# ---  PARAMÈTRES GLOBAUX ---

# Seuil de population pour garder une commune
SEUIL_POPULATION = 3500

# Définition des blocs politiques (listes des nuances à agréger)
BLOC_VERT_2020 = ['LUG', 'LECO', 'LDVG']
BLOC_GAUCHE_2014 = ['LUG', 'LSOC', 'LDVG'] # À vérifier selon les nuances de 2014

# Mettre ici les vrais noms de fichiers data
PATHS = {
    'elec_2020': 'data/elections_municipales_2020.csv',
    'elec_2014': 'data/elections_municipales_2014.csv',
    'pop_2019': 'data/DS_RP_POPULATION_COMP_2019.csv',
    'pop_2013': 'data/DS_RP_POPULATION_COMP_2013.csv',
    'diplo_2019': 'data/DS_RP_DIPLOMES_PRINC_2019.csv',
    'diplo_2013': 'data/DS_RP_DIPLOMES_PRINC_2013.csv',
    'filo_2019': 'data/DS_FILOSOFI_CC_2019.csv',
    'filo_2013': 'data/DS_FILOSOFI_CC_2013.csv'
}

# Mettre ici les vrais noms des colonnes dans les fichiers
COLS = {
    'code_insee': 'CODGEO',      # Nom de la colonne pour le code INSEE (commune)
    'code_dept': 'Code du département', # Nom de la colonne pour le code département
    'niv_geo': 'NIVGEO',         # Nom de la colonne de niveau géographique (ex: 'COM')
    'pop_totale': 'P19_POP',      # Nom de la colonne population totale (Recensement)
    'pop_cadres': 'P19_CADRE',    # Nom de la colonne pour le nombre de cadres
    'pop_diplomes': 'P19_DIPL_SUP', # Nom de la colonne pour le nombre de diplômés du sup
    'revenu_median': 'MED19',      # Nom de la colonne pour le revenu médian (Filosofi)
    'elec_pop': 'Population',    # Nom de la colonne population (fichier élections)
    'elec_nuance': 'Nuance Liste', # Nom de la colonne nuance (fichier élections)
    'elec_voix': 'Voix',         # Nom de la colonne voix (fichier élections)
    'elec_exprimes': 'Exprimés'  # Nom de la colonne exprimés (fichier élections)
}

# ---  FONCTIONS DE NETTOYAGE ROBUSTES ---

def safe_read_csv(path):
    """
    Tente de lire un CSV avec les configurations françaises les plus probables.
    Gère les erreurs d'encodage et de séparateur.
    """
    try:
        # Essai n°1 (le plus probable pour l'INSEE)
        return pd.read_csv(path, sep=';', dtype={COLS['code_insee']: str}, encoding='latin1')
    except UnicodeDecodeError:
        print(f"Alerte (non bloquant) sur {path}: latin1 a échoué, essai avec utf-8...")
        try:
            # Essai n°2 (standard)
            return pd.read_csv(path, sep=';', dtype={COLS['code_insee']: str}, encoding='utf-8')
        except Exception as e_utf8:
            print(f"ERREUR FATALE: Impossible de lire le fichier '{path}' avec encodage utf-8. Erreur: {e_utf8}")
            sys.exit() # Arrête le script
    except FileNotFoundError:
        print(f"ERREUR FATALE: Le fichier '{path}' n'a pas été trouvé.")
        print("Vérifiez le dictionnaire 'PATHS' et que le fichier est dans le dossier 'data/'.")
        sys.exit() # Arrête le script
    except Exception as e:
        print(f"ERREUR FATALE: Problème inconnu lors de la lecture de '{path}'. Erreur: {e}")
        sys.exit() # Arrête le script

def clean_departement(df):
    # Nettoie les codes départements, garde seulement FFrance métropolitaine
    print("Nettoyage des codes départements...")
    if COLS['code_dept'] not in df.columns:
        print(f"Alerte: Colonne '{COLS['code_dept']}' non trouvée. Nettoyage département sauté.")
        return df
        
    # ??? -> C'est la bonne méthode pour gérer la Corse ('2A', '2B')
    df[COLS['code_dept']] = df[COLS['code_dept']].astype(str).replace('2A', '201T')
    df[COLS['code_dept']] = df[COLS['code_dept']].replace('2B', '202T')
    df[COLS['code_dept']] = pd.to_numeric(df[COLS['code_dept']].str.replace('T', ''), errors='coerce').astype('Int64')
    df = df.dropna(subset=[COLS['code_dept']])
    # exclure DROM > 900 
    df = df[df[COLS['code_dept']] < 900]
    return df

def load_elections(path, year, nuances_bloc):
    #charge données électorales, filtre et agrège les blocs politiques
    print(f"Chargement des élections {year}...")
    
    # Utilise la fonction de lecture "blindée"
    df = safe_read_csv(path) 
    
    # Nettoyage et filtre
    df = clean_departement(df)
    
    # *** BLINDAGE : Conversion en numérique avant de filtrer ***
    df[COLS['elec_pop']] = pd.to_numeric(df[COLS['elec_pop']], errors='coerce')
    df = df.dropna(subset=[COLS['elec_pop']]) # Enlève les pop non valides
    
    df = df[df[COLS['elec_pop']] >= SEUIL_POPULATION]

    # *** BLINDAGE : Conversion en numérique des colonnes de vote ***
    df[COLS['elec_voix']] = pd.to_numeric(df[COLS['elec_voix']], errors='coerce').fillna(0)
    df[COLS['elec_exprimes']] = pd.to_numeric(df[COLS['elec_exprimes']], errors='coerce').fillna(0)

    # Agrégation des scores
    commune_group = df.groupby(COLS['code_insee'])
    voix_bloc = commune_group.apply(lambda x: x[x[COLS['elec_nuance']].isin(nuances_bloc)][COLS['elec_voix']].sum())
    voix_totales = commune_group[COLS['elec_exprimes']].sum()
    
    # Éviter les divisions par zéro si une commune n'a pas d'exprimés
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        # *** BLINDAGE : .replace(0, pd.NA) gère la division par zéro ***
        score_bloc = (voix_bloc / voix_totales.replace(0, pd.NA)) * 100
    
    df_agg = pd.DataFrame({
        f'Score_Bloc_{year}': score_bloc.fillna(0) # Mettre 0 si pas de voix ou 0 exprimés
    }).reset_index()
    
    print(f"-> Fichier élections {year} chargé. {len(df_agg)} communes retenues.")
    return df_agg

def load_census(path, year, var_col_name, pop_col_name, new_name):
    """Charge les données du Recensement (Cadres ou Diplômés)."""
    print(f"Chargement Recensement {year} ({new_name})...")
    
    # Utilise la fonction de lecture "blindée"
    df = safe_read_csv(path)
    
    # On garde que le niveau communal
    df = df[df[COLS['niv_geo']] == 'COM']
    
    # *** BLINDAGE : Conversion en numérique avant calcul ***
    var_col = pd.to_numeric(df[var_col_name], errors='coerce')
    pop_col = pd.to_numeric(df[pop_col_name], errors='coerce')

    # *** BLINDAGE : Gère la division par zéro ***
    df[new_name] = (var_col / pop_col.replace(0, pd.NA)) * 100
    
    df = df[[COLS['code_insee'], new_name]]
    return df

def load_filosofi(path, year, var_col_name, new_name):
    # charges données revenu FILOSOFI
    print(f"Chargement FILOSOFI {year} ({new_name})...")
    
    # Utilise la fonction de lecture "blindée"
    df = safe_read_csv(path)
    
    # On garde que le niveau communal
    df = df[df[COLS['niv_geo']] == 'COM']
    
    # *** BLINDAGE : Gère les virgules françaises pour les décimales ***
    var_col = df[var_col_name]
    if var_col.dtype == 'object':
        print(f"  > Détection de texte dans {new_name}, conversion de ',' en '.'...")
        var_col = var_col.str.replace(',', '.', regex=False)
        
    df[new_name] = pd.to_numeric(var_col, errors='coerce')
    
    df = df[[COLS['code_insee'], new_name]]
    return df

# ---  FONCTION PRINCIPALE ---
# (exécute tout)

def main():
    # Chargement fusion et création des variables (deltas)
    print("--- Démarrage du pipeline de préparation des données ---")

    # --- ÉTAPE 1 : CHARGEMENT DE TOUTES LES BRIQUES ---
    
    # Élections
    df_elec_2020 = load_elections(PATHS['elec_2020'], 2020, BLOC_VERT_2020)
    df_elec_2014 = load_elections(PATHS['elec_2014'], 2014, BLOC_GAUCHE_2014)

    # Recensement - Cadres (PCS)
    df_cadres_2019 = load_census(PATHS['pop_2019'], 2019, COLS['pop_cadres'], COLS['pop_totale'], 'Part_Cadres_2019')
    df_cadres_2013 = load_census(PATHS['pop_2013'], 2013, COLS['pop_cadres'], COLS['pop_totale'], 'Part_Cadres_2013')
    
    # Recensement - Diplômes
    df_diplo_2019 = load_census(PATHS['diplo_2019'], 2019, COLS['pop_diplomes'], COLS['pop_totale'], 'Part_Diplomes_2019')
    df_diplo_2013 = load_census(PATHS['diplo_2013'], 2013, COLS['pop_diplomes'], COLS['pop_totale'], 'Part_Diplomes_2013')

    # Revenus - Filosofi
    df_filo_2019 = load_filosofi(PATHS['filo_2019'], 2019, COLS['revenu_median'], 'Revenu_Median_2019')
    df_filo_2013 = load_filosofi(PATHS['filo_2013'], 2013, COLS['revenu_median'], 'Revenu_Median_2013')

    # --- ÉTAPE 2 : FUSION ---
    print("Fusion de toutes les sources de données...")
    
    # On rassemble toutes les briques dans une liste
    dfs_to_merge = {
        'elec_2014': df_elec_2014,
        'cadres_2019': df_cadres_2019,
        'cadres_2013': df_cadres_2013,
        'diplo_2019': df_diplo_2019,
        'diplo_2013': df_diplo_2013,
        'filo_2019': df_filo_2019,
        'filo_2013': df_filo_2013
    }

    # On part de la brique 2020 et on fusionne tout le reste
    master_df = df_elec_2020
    print(f"Shape de base (elec 2020): {master_df.shape}")

    # *** BLINDAGE : Ajout de logging après chaque fusion ***
    for name, df_to_merge in dfs_to_merge.items():
        master_df = pd.merge(master_df, df_to_merge, on=COLS['code_insee'], how='inner')
        print(f"  > Après fusion avec '{name}', shape: {master_df.shape}")
        if master_df.empty:
            print(f"ERREUR FATALE: La fusion avec '{name}' a produit un DataFrame vide.")
            print(f"  Vérifiez que la colonne '{COLS['code_insee']}' est au bon format (str) et existe dans tous les fichiers.")
            sys.exit()

    print(f"Fusion terminée. {len(master_df)} communes sont complètes dans tous les datasets.")

    # --- ÉTAPE 3 : CALCUL DES "DELTAS" (Le Choc) ---
    print("Calcul des variables 'Delta' (Choc de gentrification)...")
    
    master_df['Delta_Cadres'] = master_df['Part_Cadres_2019'] - master_df['Part_Cadres_2013']
    master_df['Delta_Diplomes'] = master_df['Part_Diplomes_2019'] - master_df['Part_Diplomes_2013']
    
    # RAPPEL MÉTHODOLOGIQUE IMPORTANT
    # On ne calcule PAS le 'Delta_Revenu' à cause des problèmes de fiabilité
    # sur Filosofi 2019 : sous estimation massive des revenus (non-prise en compte de la PEPA prime exceptionnelle, défiscalisation des heures supp, etc.). Insee: "le revenu médian semble stagner -0.2% alors qu'il a en réalité augmenter +2.6%"
    # On garde les variables 'Revenu_Median_2019' et 'Revenu_Median_2013' 
    # pour les analyser en tant que "Stock", ça reste correct, mais le flux serait sous estimé

    # --- ÉTAPE 4 : VÉRIFICATION FINALE ET SAUVEGARDE ---
    print("Vérification des données manquantes (NaN) avant sauvegarde...")
    print(master_df.isnull().sum())
    
    master_df_final = master_df.dropna()
    print(f"Taille finale après suppression des lignes avec NaN: {len(master_df_final)}")

    output_path = 'master_data.csv'
    master_df_final.to_csv(output_path, index=False)
    
    print("--- Pipeline terminé ! ---")
    print(f"Le fichier final '{output_path}' est prêt pour l'analyse.")
    print(f"Il contient {len(master_df_final)} communes et {len(master_df_final.columns)} variables.")
    print(master_df_final.head())


# ---  EXÉCUTION DU SCRIPT ---
# lancer le script depuis le terminal avec "python prepare_data.py"
# ==========================================================

if __name__ == "__main__":
    main()
