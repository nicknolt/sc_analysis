# calcul des poinds individuels / moyens et perte de poids indiv / moyenne
# Data_SC / cache / XP_events.csv données corrigées à partir du sniffer local

# attention aux périodes étudiées (tout par défaut, y compris J0 pour le poids initial)
#********************************


# Charger la bibliothèque dplyr
library(dplyr)
library(ggplot2)
library(lubridate)
library(jsonlite)

args = commandArgs(trailingOnly=TRUE)
print(args)
json <- args[1]

print(json)

json_conf <- fromJSON(json)

pdf(NULL)


csv_file = json_conf['csv_file']
figure_file = json_conf['figure_file']
# action_type = args[3]

##### lecture fichier
# filename = r"(C:\Users\Nicolas\PycharmProjects\SC_Analysis\tests\resources\cache\XP11\XP11_mice_weight.csv)"
# print(filename)
# a=read.csv(filename, sep=",",header = TRUE)
a=read.csv(args[1],sep=",",header = TRUE)

a <- a %>%
  select(rfid, weight, day_since_start) 
  
###attention # des souris doivent être facteur et non integer
a$rfid <- as.factor(a$rfid)
# a$action <- as.factor(a$action)
a$weight <- as.numeric(a$weight)
a$day_since_start <- as.integer(a$day_since_start)

a <- a
  #filter(day_since_start >= 0 & day_since_start <= 22)

## calcul du poids moyen : par cohorte au cours du temps -> by day_since_start

Poids_moyen <- a %>%
  # filter(rfid != "0" & action == "transition") %>%
  group_by(day_since_start) %>%
  summarize(av_weight = mean(weight),
            SEM = sd(weight) / sqrt(n()))

ggplot(Poids_moyen, aes(x = as.numeric(day_since_start), y = as.numeric(av_weight))) +
  geom_line() +
  geom_errorbar(
    aes(ymin = av_weight - SEM, ymax = av_weight + SEM),
    width = 0.2, # Largeur des barres d'erreur
  ) +
  labs(x = "Jour", y = "Poids (g)") +
  #facet_wrap(~rfid, scales = "fixed") + #par souris ou sur le meme graphe
  theme_minimal()

ggsave(paste(figure_file, "_pd_moyen", ".jpg"))

# Moyenne par jour  [# de ligne= J+1 (commence à 1 et on J0)]
# print(Poids$av_weight[20])

## calcul du poids individuel moyen au cours du temps

Indiv_Poids <- a %>%
  # filter(rfid != "0" & action == "transition") %>%
  # filter(rfid != "0") %>%
  group_by(day_since_start, rfid) %>%
  #group_by(day_since_start) %>%
  summarize(av_weight = mean(weight),
            SEM = sd(weight) / sqrt(n()))

ggplot(Indiv_Poids, aes(x = as.numeric(day_since_start), y = as.numeric(av_weight), group = rfid)) +
  geom_line() +
  geom_errorbar(
    aes(ymin = av_weight - SEM, ymax = av_weight + SEM),
    width = 0.2, # Largeur des barres d'erreur
  ) +
  labs(x = "Jour", y = "Poids (g)") +
 #facet_wrap(~rfid, scales = "fixed") + #par souris ou sur le meme graphe
  theme_minimal()

ggsave(paste(figure_file, "_individuel", ".jpg"))

# POids individuel par jour et par souris [# de ligne]
print(Indiv_Poids$av_weight[3])



##### calcul des poids individuels au jour actuel
n_weight <- Indiv_Poids %>%
  filter(day_since_start == max(Indiv_Poids$day_since_start)) %>%
  group_by(rfid) %>%
  summarize(mean_n_weight = mean(av_weight))

print(n_weight$mean_n_weight)

##### calcul des poids de référence à J0
Ref_weight <- Indiv_Poids %>%
  filter(day_since_start == min(Indiv_Poids$day_since_start)) %>%
  group_by(rfid) %>%
  summarize(ref_weight = mean(av_weight))

print(Ref_weight$ref_weight)

# Joindre la table des Poids individuels avec les valeurs de référence
w_loss <- Indiv_Poids %>%
  left_join(Ref_weight, by = "rfid") %>%
  mutate(Pct_loss = ((av_weight / ref_weight) * 100) -100) %>%
  arrange(rfid)

avg_w_loss<- w_loss %>%
  group_by(day_since_start) %>%
  summarize(avg_loss = mean(Pct_loss),
            SEM2 = sd(Pct_loss) / sqrt(n()))

# Créer un graphique du % perte par rapport au 1er jour  et par souris en utilisant le nombre de jours sur l'axe des abscisses
#ggplot(w_loss, aes(x = as.numeric(day_since_start), y = as.numeric(Pct_loss), color = rfid)) +
ggplot(avg_w_loss, aes(x = as.numeric(day_since_start), y = as.numeric(avg_loss))) + #enlever les errorbars si enlevé
  geom_line() +
  geom_errorbar(
    aes(ymin = avg_loss - SEM2, ymax = avg_loss + SEM2),
    width = 0.2, # Largeur des barres d'erreur
  ) +
  labs(x = "Jour", y = "% weight loss ") +
  #facet_wrap(~rfid, scales = "fixed") + #par souris ou sur le meme graphe
  theme_minimal()
ggsave(paste(figure_file, "_avg_pct_loss", ".jpg"))

# Créer un graphique du % perte par rapport au 1er jour  et par souris en utilisant le nombre de jours sur l'axe des abscisses
ggplot(w_loss, aes(x = as.numeric(day_since_start), y = as.numeric(Pct_loss), color = rfid)) +
  geom_line() +
  labs(x = "Jour", y = "% weight loss ") +
  #facet_wrap(~rfid, scales = "fixed") + #par souris ou sur le meme graphe
  theme_minimal()

ggsave(paste(figure_file, "_pct_loss", ".jpg"))
