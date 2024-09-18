# calcul du nb d'appuis/ souris au cours du temps / moyen au cours du temps / total au cours du temps
# Data_SC / cache / XP_sequences.csv données corrigées à partir du sniffer local

# Charger la bibliothèque dplyr
library(dplyr)
library(ggplot2)
library(lubridate)

pdf(NULL)

print("KIKOOLOL")

args = commandArgs(trailingOnly=TRUE)
csv_file = args[1]
figure_file = args[2]
action_type = args[3]

##### lecture fichier

a=read.csv(args[1],sep=",",header = TRUE)
a$rfid <- as.factor(a$rfid)
a$time <- ymd_hms(a$time, tz = Sys.timezone())
a$time_next_action <- ymd_hms(a$time_next_action, tz = Sys.timezone())
a$next_action <- as.factor(a$next_action)
a$duration <- as.numeric(a$duration)

a <- a %>%
  #filter(day_since_start >= 15 & day_since_start <= 22 & rfid != "0") 
  filter(day_since_start!= 0 & day_since_start <= 8 & rfid != "0") 


if (action_type=='LEVER_PRESS'){

    print("LEVER PRESS!!")
    # Filtrer uniquement les actions où l'action est LP
    df_LP <- a %>%
      filter(action == "id_lever")
}else if(action_type=="TRANSITION"){

    # Filtrer uniquement les actions où l'action est LP
    df_LP <- a %>%
      filter(action == "transition")
}



# Calculer la distribution des actions suivantes après Action 1 pour chaque souris
distribution <- df_LP %>%
  group_by(rfid, next_action) %>%
  summarise(count = n()) %>%
  mutate(percentage = count / sum(count) * 100)

# Fonction pour créer un Pie Chart pour chaque souris
ggplot(distribution, aes(x = "", y = percentage, fill = next_action)) +
    geom_bar(stat = "identity", width = 1) +
    coord_polar("y", start = 0) +
    facet_wrap(~rfid, scales = "fixed") + #par souris ou sur le meme graphe
    theme_void() +
    theme(legend.position = "right")

ggsave(figure_file)
