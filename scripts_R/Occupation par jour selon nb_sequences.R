# calcul du nb d'appuis/ souris au cours du temps / moyen au cours du temps / total au cours du temps
# Data_SC / cache / XP_sequences.csv données corrigées à partir du sniffer local

# Charger la bibliothèque dplyr
library(dplyr)
library(ggplot2)
library(lubridate)


##### lecture fichier

a=read.csv("/Users/macminicv/Documents/Data_SC/cache/XP11/XP11_occupation_time.csv",sep=",",header = TRUE) 
a$nb_mice <- as.factor(a$nb_mice)
a$mouse <- as.factor(a$mouse)


a <- a %>%
  filter(day_since_start != 0 & day_since_start <= 22 & mouse != "0" & mouse != "EMPTY") 
  #filter(!is.na(rfid_np)  & rfid_lp != 0 & as.character(rfid_lp) == as.character(rfid_np))

ggplot(a, aes(x = (day_since_start), y = as.numeric(duration), colour =nb_mice)) +
  geom_line(size=0.5) +
  xlab("Days") +
  ylab("Time (s) in LMT") +
  #facet_wrap(~mouse, scales = "fixed") +
  #facet_wrap(~mouse, scales = "free_y") +
  theme_minimal()


# Calcul du temps passé seule dans lmT 

onemouse <- a %>%
  group_by(day_since_start, mouse) %>%
  filter(nb_mice == 1)
    
ggplot(onemouse, aes(x = (day_since_start), y = as.numeric(duration), color=mouse)) +
  geom_line(size=0.5) +
  xlab("Days") +
  ylab("Number of lever press") +
  #facet_wrap(~mouse, scales = "fixed") +
  #facet_wrap(~mouse, scales = "free_y") +
  theme_minimal()

moyenne <- a %>%
  group_by(day_since_start) %>%
  tally

moyenne <- onemouse %>%
  group_by(day_since_start) %>%
  summarize(
    count = n(),
    mean_barpresses = mean(n),
    sd_barpresses = sd(n),
    SEM = sd_barpresses / sqrt(n()))

ggplot(moyenne, aes(x = (day_since_start), y = (mean_barpresses))) +
  geom_line(size=0.5) +
  geom_errorbar(aes(ymin = mean_barpresses - SEM, ymax = mean_barpresses + SEM),
    width = 0.2) + # Largeur des barres d'erreur
  xlab("Days") +
  ylab("Number of lever press") +
  #facet_wrap(~rfid_lp, scales = "fixed") +
  #facet_wrap(~Mouse, scales = "free_y") +
  theme_minimal()

somme <- profil %>%
  group_by(day_since_start) %>%
  summarize(
    count = n(),
    total_barpresses = sum(n),
    sd_barpresses = sd(n),
    SEM = sd_barpresses / sqrt(n()))

ggplot(somme, aes(x = (day_since_start), y = (total_barpresses))) +
  geom_line(size=0.5) +
  geom_errorbar(aes(ymin = total_barpresses - SEM, ymax = total_barpresses + SEM),
                width = 0.2) + # Largeur des barres d'erreur
  xlab("Days") +
  ylab("Number of lever press") +
  #facet_wrap(~rfid_lp, scales = "fixed") +
  #facet_wrap(~Mouse, scales = "free_y") +
  theme_minimal()
