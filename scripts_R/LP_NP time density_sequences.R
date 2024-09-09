# calcul du critère pour séquences completes
# Data_SC / cache / XP_sequences.csv données corrigées à partir du sniffer local

# Charger la bibliothèque dplyr
library(dplyr)
library(ggplot2)
library(lubridate)


##### lecture fichier

a=read.csv("/Users/macminicv/Documents/Data_SC/cache/XP10/XP10_sequences.csv",sep=",",header = TRUE) 
a$rfid_np <- as.factor(a$rfid_np)
a$rfid_lp = as.factor(a$rfid_lp)

#  Calcul de la distribution du délai LP-NP _ seulement sequences completes (même souris)
a <- a %>%
  filter(day_since_start != 0 & day_since_start <= 22) %>%
  filter(!is.na(rfid_np)  & rfid_lp != 0 & as.character(rfid_lp) == as.character(rfid_np))

# Calculer le 80ème percentile (la valeur maximale en dessous de laquelle on trouve 80% des données)
percentile_80 <- quantile(a$elapsed_s, 0.80, na.rm = TRUE)

ggplot(a, aes(x = elapsed_s)) +
  geom_density(fill = "blue", alpha = 0.3) +  # Tracer la courbe de densité
  geom_vline(aes(xintercept = percentile_80), color = "red", linetype = "dashed", size = 1) +  # Ajouter une ligne verticale pour le 80ème percentile
  labs(title = "Density of elapsed_s with 80th percentile", x = "elapsed_s (seconds)", y = "Density") +
  #coord_cartesian(xlim = c(min(a$elapsed_s), percentile_80 + 10)) +  # Zoomer sur une plage autour des valeurs importantes
  xlim(min(a$elapsed_s), percentile_80 + 10) +
  theme_minimal()

cat("80% des valeurs sont en dessous de :", percentile_80, "secondes\n")

# 