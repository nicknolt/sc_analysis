# calcul du nb d'appuis/ souris au cours du temps / moyen au cours du temps / total au cours du temps
# Data_SC / cache / XP_sequences.csv données corrigées à partir du sniffer local

# Charger la bibliothèque dplyr
library(dplyr)
library(ggplot2)
library(lubridate)


##### lecture fichier

a=read.csv("/Users/macminicv/Documents/Data_SC/cache/XP11/XP11_3_sequences.csv",sep=",",header = TRUE) 
a$rfid_lp <- as.factor(a$rfid_lp)
a$complete_sequence <- (a$complete_sequence)
a$lever_press_dt <- ymd_hms(a$lever_press_dt, tz = Sys.timezone())

a <- a %>%
  #filter(day_since_start >= 15 & day_since_start <= 22 & rfid_lp != "0") 
  filter(day_since_start!= 0 & day_since_start <= 22 & rfid_lp != "0") 

# Calcul du nb de séquences complètes

appuis_solo <- a %>%
  group_by(rfid_lp, day_since_start, complete_sequence = "True") %>%
  count(rfid_lp)

ggplot(appuis_solo, aes(x = (day_since_start), y = as.numeric(n), colour =rfid_lp)) +
#ggplot(appuis_solo, aes(x = (day_since_start), y = as.numeric(n))) +
  geom_line(size=0.5) +
  xlab("Days") +
  ylab("Time (s) in LMT") +
  facet_wrap(~complete_sequence, scales = "fixed") +
  #facet_wrap(~mouse, scales = "free_y") +
  theme_minimal()


##### lecture fichier

cs=read.csv("/Users/macminicv/Documents/Data_SC/cache/XP11/XP11_percentage_complete_sequence.csv",sep=",",header = TRUE) 
cs$rfid_lp <- as.factor(cs$rfid_lp)
cs$percent_complete_sequence <- as.numeric(cs$percent_complete_sequence)
cs$total_per_day <- as.numeric(cs$total_per_day)
cs$lever_press_dt <- ymd_hms(cs$lever_press_dt, tz = Sys.timezone())


cs <- cs %>%
  filter(day_since_start != 0 & day_since_start <= 22 & rfid_lp != "0") 
#filter(!is.na(rfid_np)  & rfid_lp != 0 & as.character(rfid_lp) == as.character(rfid_np))

# Calcul du pourcentage de séquences complètes par souris par jour

ggplot(cs, aes(x = (day_since_start), y = as.numeric(percent_complete_sequence), colour =rfid_lp)) +
  #ggplot(appuis_solo, aes(x = (day_since_start), y = as.numeric(n))) +
  geom_line(size=0.5) +
  xlab("Days") +
  ylab("% complete sequences") +
  #facet_wrap(~rfid_lp, scales = "fixed") +
  #facet_wrap(~rfid_lp, scales = "free_y") +
  theme_minimal()
