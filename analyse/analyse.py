# BUT: analyser le 'master_data.csv' propre et tester nos hypothèses.

import pandas as pd
import statsmodels.formula.api as smf #bibliothèque pour les régressions
import sys

# Nom du fichier de données en entrée (celui créé par prepare_data.py)
INPUT_DATA = 'master_data.csv'

def main():
    """
    Fonction principale qui charge les données,
    définit les modèles et affiche les résultats.
    """
    print("--- Démarrage de l'analyse statistique ---")

    # --- ÉTAPE 1 : CHARGEMENT DES DONNÉES ---
    print(f"Chargement du fichier de données propre : {INPUT_DATA}")
    
    try:
        data = pd.read_csv(INPUT_DATA)
    except FileNotFoundError:
        print("\nERREUR FATALE : Le fichier 'master_data.csv' n'a pas été trouvé.")
        print("Veuillez d'abord lancer 'python prepare_data.py' pour le générer.")
        sys.exit() # Arrête le script
    except Exception as e:
        print(f"\nERREUR FATALE : Impossible de lire 'master_data.csv'. Erreur: {e}")
        sys.exit()

    print(f"Données chargées. {len(data)} communes prêtes pour l'analyse.")

    # --- ÉTAPE 2 : DÉFINITION ET CALCUL DES MODÈLES ---
    # C'est ici qu'on teste nos hypothèses
    
    # ---
    # MODÈLE 1 : Hypothèse "Stock"
    # Le vote 2020 s'explique-t-il par le niveau de gentrification de 2013 ?
    # ---
    print("\n--- CALCUL DU MODÈLE 1 : 'Stock' (Niveau 2013) ---")
    
    # On utilise le format R : Y ~ X1 + X2 + X3
    formula_stock = """
        Score_Bloc_2020 ~ 
            Part_Cadres_2013 + 
            Part_Diplomes_2013 + 
            Revenu_Median_2013
    """
    
    model_stock = smf.ols(formula_stock, data=data).fit()

    # ---
    # MODÈLE 2 : Hypothèse "Choc"
    # Le vote 2020 s'explique-t-il par la *vitesse* du changement (le Delta) ?
    # ---
    print("\n--- CALCUL DU MODÈLE 2 : 'Choc' (Delta 2013-2019) ---")
    
    formula_choc = """
        Score_Bloc_2020 ~ 
            Delta_Cadres + 
            Delta_Diplomes
    """
    # (Rappel : on n'inclut pas Delta_Revenu à cause des problèmes de fiabilité de 2019)
    
    model_choc = smf.ols(formula_choc, data=data).fit()

    # ---
    # MODÈLE 3 : Hypothèse "Complet (Choc vs. Stock)"
    # On met tout en compétition pour voir ce qui est le plus prédictif.
    # ---
    print("\n--- CALCUL DU MODÈLE 3 : 'Complet (Choc vs. Stock)' ---")
    
    formula_complet = """
        Score_Bloc_2020 ~ 
            Part_Cadres_2013 + Delta_Cadres +
            Part_Diplomes_2013 + Delta_Diplomes +
            Revenu_Median_2019
    """
    # Note: On utilise Revenu_Median_2019 ici comme la meilleure mesure du "Stock" de revenu actuel
    
    model_complet = smf.ols(formula_complet, data=data).fit()

    # --- ÉTAPE 3 : AFFICHAGE DES RÉSULTATS ---
    print("\n\n" + "="*50)
    print("         RÉSULTATS DE L'ANALYSE DE RÉGRESSION")
    print("="*50)

    print("\n\n--- MODÈLE 1 : 'Stock' (Niveau 2013) ---")
    print(model_stock.summary())
    print("\n(Analyse: Regardez le R-squared. Est-il élevé ? Les variables sont-elles significatives P>|t| < 0.05 ?)")

    print("\n\n--- MODÈLE 2 : 'Choc' (Delta 2013-2019) ---")
    print(model_choc.summary())
    print("\n(Analyse: Le R-squared est-il meilleur que le Modèle 1 ? Les Deltas sont-ils significatifs ?)")

    print("\n\n--- MODÈLE 3 : 'Complet (Choc vs. Stock)' ---")
    print(model_complet.summary())
    print("\n(Analyse: Comparez le R-squared ! Voyez-vous les variables 'Stock' (ex: Part_Cadres_2013) ")
    print("  et 'Choc' (ex: Delta_Cadres) être *toutes les deux* significatives ?")
    print("  Si 'Delta_Cadres' est significatif, vous avez prouvé l'importance du 'choc'.")

# ---  EXÉCUTION DU SCRIPT ---
if __name__ == "__main__":
    main()
