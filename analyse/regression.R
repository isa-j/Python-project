# nalyse de régression - Projet Delta Gentrification 


file_path <- "/Users/romain/Desktop/Projets DS/Python-project/analyse/data/data_deltas_pour_regression.csv"


df <- read.csv2(file_path)
  
# conversion colonne en numérique
cols_to_numeric <- setdiff(names(df), "COM")
df[cols_to_numeric] <- lapply(df[cols_to_numeric], as.numeric)
  
cat("Données chargées OK")
cat(paste("Nombre total de communes :", nrow(df)))
  

# Analyse descriptive 
cat("Summary, analyse descriptive")

print(summary(df))


# Regression linéaire MCO

cat("Resultats MCO")


# On explique Y (Score 2020) par les Deltas (X), les Stocks (X) et le Vote passé (X)
formula <- Score_Gauche_Ecolo_2020 ~ Delta_Cadres + 
  Delta_Diplomes + 
  MED19 + 
  MED13 + 
  P20_POP + 
  Score_Bloc_Gauche_2014


model <- lm(formula, data = df)

# Affichage du résumé complet du modèle

print(summary(model))