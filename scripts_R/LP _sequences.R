# calcul du nb d'appuis/ souris au cours du temps / moyen au cours du temps / total au cours du temps
# Data_SC / cache / XP_sequences.csv données corrigées à partir du sniffer local

# Charger la bibliothèque dplyr
library(dplyr)
library(ggplot2)
library(lubridate)


##### lecture fichier

a=read.csv("/Users/macminicv/Documents/Data_SC/cache/XP11/XP11_one_step_seq_LEVER_PRESS.csv",sep=",",header = TRUE) 
a$rfid <- as.factor(a$rfid)
a$time <- ymd_hms(a$time, tz = Sys.timezone())
a$time_next_action <- ymd_hms(a$time_next_action, tz = Sys.timezone())
a$next_action <- as.factor(a$next_action)
a$duration <- as.numeric(a$duration)

a <- a %>%
  #filter(day_since_start >= 15 & day_since_start <= 22 & rfid != "0") 
  filter(day_since_start!= 0 & day_since_start <= 22 & rfid != "0") 


# Filtrer uniquement les actions où l'action est LP
df_LP <- a %>% 
  filter(action == "id_lever")

# Calculer la distribution des actions suivantes après Action 1 pour chaque souris
distribution <- df_LP %>%
  group_by(rfid, next_action) %>%
  summarise(count = n()) %>%
  mutate(percentage = count / sum(count) * 100)

# Fonction pour créer un Pie Chart pour chaque souris
ggplot(distribution, aes(x = "", y = percentage, fill = factor(next_action))) +
    geom_bar(stat = "identity", width = 1) +
    coord_polar("y", start = 0) +
    labs(title = paste("Souris", rfid), fill = "Action Suivante") +
    theme_void() +
    theme(legend.position = "right")


# Calcul de la moyenne des pourcentages pour toutes les souris
average_distribution <- df_LP %>%
  group_by(next_action) %>%
  summarise(count = n()) %>%
  mutate(percentage = count / sum(count) * 100)

average_pie_chart <- ggplot(average_distribution, aes(x = "", y = percentage, fill = factor(next_action))) +
  geom_bar(stat = "identity", width = 1) +
  coord_polar("y", start = 0) +
  labs(title = "Distribution Moyenne", fill = "Action Suivante") +
  theme_void() +
  theme(legend.position = "right")

# ----- Ajout de la représentation en histogrammes -----

# Représentation en histogrammes pour chaque souris
histogram_plot <- ggplot(df_LP, aes(x = factor(next_action), fill = factor(next_action))) +
  geom_bar(position = "dodge") +
  facet_wrap(~rfid, scales = "free_y") +
  labs(title = "Distribution des Actions Suivantes après Action 1 pour chaque souris",
       x = "Action Suivante",
       y = "Fréquence") +
  theme_minimal()

# Représentation moyenne en histogrammes
average_histogram <- ggplot(average_distribution, aes(x = factor(next_action), y = percentage, fill = factor(next_action))) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(title = "Distribution Moyenne des Actions Suivantes",
       x = "Action Suivante",
       y = "Pourcentage") +
  theme_minimal()


group_by(rfid, day_since_start, time_next_action, duration) %>%
  count(NP = (next_action == "nose_poke"),
        LP = (next_action == "id_lever"),
        Trans = (next_action == "transition"))

# Calcul 

post_LP <- a[a$action == "id_lever",]
distribution <- table(post_LP$rfid, post_LP$next_action) 
distribution_percentage <- prop.table(distribution, margin = 1) * 100
distribution_percentage

postLP_2 <- a %>%
  group_by(rfid, day_since_start, time_next_action, duration) %>%
  summarize(time_next_action[nose_poke], time_next_action[id_lever], time_next_action[transition])

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
