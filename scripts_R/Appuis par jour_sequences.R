# calcul du nb d'appuis/ souris au cours du temps / moyen au cours du temps / total au cours du temps
# Data_SC / cache / XP_sequences.csv données corrigées à partir du sniffer local

# Charger la bibliothèque dplyr
library(dplyr)
library(ggplot2)
library(lubridate)


##### lecture fichier

a=read.csv("/Users/macminicv/Documents/Data_SC/cache/XP11T2/XP11T2_5_sequences.csv",sep=",",header = TRUE) 
a$rfid_np <- as.factor(a$rfid_np)
a$rfid_lp = as.factor(a$rfid_lp)
a$lever_press_dt <- ymd_hms(a$lever_press_dt, tz = Sys.timezone())

a <- a %>%
  filter(day_since_start != 0 & day_since_start <= 22 & rfid_lp != "0") 
  #filter(!is.na(rfid_np)  & rfid_lp != 0 & as.character(rfid_lp) == as.character(rfid_np))

# Calcul du nb d'appuis

profil <- a %>%
  group_by(rfid_lp, day_since_start) %>%
  count(rfid_lp)
    
ggplot(profil, aes(x = (day_since_start), y = as.numeric(n), color=rfid_lp)) +
  geom_line(size=0.5) +
  xlab("Days") +
  ylab("Number of lever press") +
  #facet_wrap(~rfid_lp, scales = "fixed") +
  #facet_wrap(~Mouse, scales = "free_y") +
  theme_minimal()

moyenne <- a %>%
  group_by(day_since_start) %>%
  tally

moyenne <- profil %>%
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
  geom_errorbar(aes(ymin = mean_barpresses - SEM, ymax = mean_barpresses + SEM),
                width = 0.2) + # Largeur des barres d'erreur
  xlab("Days") +
  ylab("Number of lever press") +
  #facet_wrap(~rfid_lp, scales = "fixed") +
  #facet_wrap(~Mouse, scales = "free_y") +
  theme_minimal()
