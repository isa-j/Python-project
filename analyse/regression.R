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