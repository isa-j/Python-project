# BUT: analyser le 'master_data.csv' propre et tester nos hypothèses par regression lineaire

import pandas as pd
import statsmodels.formula.api as smf #bibliothèque pour les régressions
import sys

# Nom du fichier de données en entrée (celui créé par prepare_data.py)
INPUT_DATA = 'master_data.csv'
