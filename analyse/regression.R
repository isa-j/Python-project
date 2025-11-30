# analyse de régression - Projet Delta Gentrification 
library(haven)
library(tidyverse)
library(lmtest)
library(sandwich)
library(car)
library(dplyr)
library(estimatr)

df <- read.csv("/Users/romain/Desktop/Projets DS/Python-project/analyse/data_pour_regression.csv")

# 
model_robust <- lm_robust(delta_elec ~ delta_pct_diplome + MED13 + delta_pop, data = df)

summary(model_robust)



# REGRESSION LOGISTIQUE (EN COURS)

#  Créer une variable binaire "Conquête"
# 1 si la ville a basculé vers votre parti (delta = +1)
# 0 sinon (si elle est restée stable ou a basculé dans l'autre sens)
df$basculement_gauche <- ifelse(df$delta_elec == 1, 1, 0)

# 2. Régression Logistique (GLM - Generalized Linear Model)
# family = "binomial" dit à R : "C'est une proba, utilise une courbe en S (sigmoïde), pas une droite".
model_logit <- glm(basculement_gauche ~ delta_pct_diplome + MED13 + delta_pop, 
                   data = df, 
                   family = "binomial")

summary(model_logit)