#ce fichier suive le fichier "utility_models" dans le processus de distances_emplois, il ajoute le nombre de trajets
library(conflicted)
conflict_prefer_all("dplyr")

library(arrow)
library(data.table)
library(tidyverse)
library(r3035)
library(mlogit)
# devtools::install_github("krlmlr/wrswoR")
data.table::setDTthreads(12)
arrow::set_cpu_count(16)

conflict_prefer("filter", "dplyr")

source("~/marseille/enq_mobilite_starter.r")

transport <- c("walk", "bike", "transit", "car")
# Mobilité locale (distance vol d'oiseau entre deux communes inférieure à 80 km)
deploc <- deploc |> 
  filter(mobloc == 1) |> 
  mutate(motif_pro = ifelse(between(MMOTIFDES, 9.0, 9.9), "travail", "autres"))

# POND_JOUR extrapole le nombre de trajets du type correspondant effectué dans la semaine sur l'ensemble
# de la population.
# pond_indC donne la pondération correspondante du nombre d'individus effectuant un tel trajet.

#### 1- nombre de trajets par individu par motif tous modes toutes distances confondues #### 
statmob <- deploc |> 
  group_by(IDENT_IND, motif_pro) |>
  mutate(distance = MDISTTOT_fin * POND_JOUR,
         duree = DUREE * POND_JOUR) |> 
  summarise(sexe = first(SEXE),
            age = first(AGE),
            cs_act = first(CS_ACT),
            actoccup = first(ACTOCCUP),
            TAA2017_RES = first(TAA2017_RES),
            DENSITECOM_RES = first(DENSITECOM_RES),
            pond_indC = first(pond_indC),
            distance = mean(distance, na.rm = TRUE),
            duree = mean(duree, na.rm = TRUE),
            nb_trajets = sum(POND_JOUR)) 

statmob <- statmob |> 
  mutate(nb_trajets = nb_trajets / pond_indC,
         distance = distance / pond_indC, 
         duree = duree / pond_indC) |> 
  ungroup()

MOD_nbtraj <- lm(nb_trajets ~ motif_pro + DENSITECOM_RES, data = statmob)
summary(MOD_nbtraj)

#### 4- nb de trajets selon distance, fonction de decay ####

deploc |> 
  filter(motif_pro == "autres") |> 
  ggplot(aes(x = MDISTTOT_fin, weight = POND_JOUR)) + 
  geom_density(bw = 2) + 
  xlim(0, 50)

dpro <- deploc |> filter(motif_pro == "travail")

MOD_dens <- density(deploc$MDISTTOT_fin,
        bw = "nrd", 
        weight = deploc$POND_JOUR/sum(deploc$POND_JOUR),
        adjust = 4, kernel = "gaussian", n = 1024) 

simul_dens <- tibble(x=MOD_dens$x, y=MOD_dens$y) |> 
  filter(x>0) |> 
  mutate(z = y / (2 * pi * x))
ggplot(simul_dens, aes(x = x)) + 
  geom_line(aes(y=y), col = "blue") + 
  xlim(0,40) + 
  geom_line(aes(y=z), col = "red")

MOD_denspro <- density(dpro$MDISTTOT_fin,
                    bw = "nrd", 
                    weight = dpro$POND_JOUR/sum(dpro$POND_JOUR),
                    adjust = 4, kernel = "gaussian", n = 1024) 

simul_denspro <- tibble(x=MOD_denspro$x, y=MOD_denspro$y) |> 
  filter(x>0) |> 
  mutate(z = y / (2 * pi * x))
ggplot(simul_denspro, aes(x = x)) + 
  geom_line(aes(y=y), col = "blue") + 
  xlim(0,40) + 
  geom_line(aes(y=z), col = "red")

simul_denspro <- simul_denspro |> filter(x < 50)
ggplot(simul_denspro, aes(x = x, y = z)) + 
  geom_line() + 
  geom_smooth(method = "lm", col = "blue") +
  geom_smooth(method = "lm", formula = y ~ poly(x, 2), col = "green") +
  scale_y_log10()
ggplot(simul_denspro, aes(x = x, y = z)) + 
  geom_line() + 
  geom_smooth(method = "lm", col = "blue") +
  geom_smooth(method = "lm", formula = y ~ poly(x, 2), col = "green") +
  scale_y_log10() + scale_x_log10()

lm(log(z) ~ x, data = simul_denspro) |> summary()
lm(log(z) ~ log(x), data = simul_denspro) |> summary()
lm(log(z) ~ log(x) , data = simul_denspro |> filter(x<10)) |> summary()
lm(log(z) ~ x, data = simul_denspro |> filter(x<10)) |> summary()

# en première approx, f_decay ~ exp(-0.1 x) 

#MOD_dens$y

f_densite <- function(x) MOD_dens$y[MOD_dens$x>x][1]
f_decay <- function(x) f_densite(x) / (2 * pi * x)

f_decay(10)/f_decay(30)
f_decay(1)/f_decay(3)


f_densitepro <- function(x) MOD_denspro$y[MOD_denspro$x>x][1]
f_decaypro <- function(x) f_densitepro(x) / (2 * pi * x)

f_decaypro(10)/f_decaypro(30)
f_decaypro(1)/f_decaypro(3)

#### 3- imputation des nb de trajets dans la matrice de distances ----

# Ajout des infos sur la densité des communes
# densite_urb <- readxl::read_xlsx("~/files/DVFdata/sources/Communes/grid.xlsx",
#                                  skip = 4)
# 
# densite_urb <- densite_urb |> 
#   select(-libgeo) |> 
#   mutate(across(.fns = as.integer)) |> 
#   as.data.table()

distances <- arrow::read_parquet("/scratch/distances/distances_emplois.parquet")
distances[, tout_mode := walk + bike + car + transit]

# distances <- densite_urb[distances, on = c("codgeo"="COMMUNE") ] |> 
#   setnames(c("codgeo", "gridens"), c("COMMUNE", "DENSITECOM_ORI"))
# 
# distances <- densite_urb[distances, on = c("codgeo"="DCLT") ] |> 
#   setnames(c("codgeo", "gridens"), c("DCLT", "DENSITECOM_DES"))

# Calcul sur le carreau de départ des nb de personnes faisant des trajets domicile-travail (mobpro)
trajets <- distances[, .(DENSITECOM_RES = first(DENSITECOM_ORI),
                         Ind18_64 = first(Ind18_64),
                         prop_ind = first(prop_ind),
                         tout_mode = first(tout_mode)),
                     by = c("fromidINS", "DCLT")]

trajets <- trajets[, .(DENSITECOM_RES = first(DENSITECOM_RES),
                         Ind18_64 = first(Ind18_64),
                         prop_ind = first(prop_ind),
                         tout_mode = sum(tout_mode)),
                   by = "fromidINS"]

trajets[, traj_freq := tout_mode * prop_ind]
# trajets[, DENSITECOM_RES := factor(DENSITECOM_RES)]

# predictions du nombre de trajets par semaine (enq. mobilité des personnes)
trajets[, nbtrajets := predict(MOD_nbtraj, newdata = cbind(trajets, motif_pro = "travail"))]
trajets[, nbtrajets := nbtrajets * Ind18_64]

# mise en rapport des deux sources
resultat <- lm(nbtrajets ~ traj_freq, data = trajets)
summary(resultat)

ggplot(trajets, aes(x=traj_freq, y=nbtrajets)) +
  geom_point(alpha = 0.1) + xlim(0, 200) + ylim(0, 3000)

# injection dans la matrice de distance des prédictions du nombre de trajets par ind
trajets <- trajets[, .(fromidINS, 
                       nbtrajets_par_ind = predict(MOD_nbtraj, newdata = cbind(trajets, motif_pro = "travail")))]

distances <- trajets[distances, on = "fromidINS"]

# nb trajets total en partance de la case i
distances[, nbtrajets_all_dest := nbtrajets_par_ind * Ind18_64]

# tout_mode est le nombre d'individu mobile entre deux communes selon mob pro
# il sert à donner les proportions pour répartir les trajets de COMMUNE vers les DCLT
prop_com_dclt <- distances[, .(prop_com_dclt = first(tout_mode)), by = c("COMMUNE", "DCLT")]
prop_com_dclt[, prop_com_dclt := prop_com_dclt / sum(prop_com_dclt), by = "COMMUNE"]

# répartition des trajets à proportion des infos de mob pro
distances <- prop_com_dclt[distances, on = c("COMMUNE", "DCLT")]
distances[, nbtrajets_par_ind_ij := nbtrajets_par_ind * prop_com_dclt * prop_emp]

# nbtrajets_par_ind_ij est un bon indicateur de ce que l'on cherche
# On peut ensuite prendre en compte la baisse du nombre de trajets selon la distance,
# sachant qu'ici cette correction apporte une simple nuance à l'intérieur des dclt
# note on utilise le même decay que pour les motifs non profesionels
exp22m_decay <- function(
    d,
    p=list(alpha1=0.1, alpha2=0.1, alpha3=0.1, alpha4=0.1, mu1=5, mu2=10, mu3=20)) {
  p <- map(p, ~ifelse(.x<0,0,.x))
  aa1 <- exp(-p$alpha1*p$mu1)
  aa2 <- aa1*exp(-p$alpha2*p$mu2)
  aa3 <- aa2*exp(-p$alpha3*p$mu3)
  if_else(d/1000 < p$mu1, exp(-d/1000*p$alpha1), 
          if_else(d/1000<p$mu1+p$mu2, aa1*exp(-p$alpha2*(d/1000-p$mu1)),
                  if_else(d/1000<p$mu1+p$mu2+p$mu3, aa2*exp(-p$alpha3*(d/1000-p$mu1-p$mu2)),
                          aa3*exp(-p$alpha4*(d/1000-(p$mu2+p$mu1+p$mu3)))))) 
}
load("abhat_exp22m_10.rda")

distances[, facteur_decay := exp22m_decay(distance_car, abhat_exp22m$par)]
distances[, nbtrajets_par_ind_ij_decay := nbtrajets_par_ind_ij * facteur_decay]
# renormalisation du nb de trajets entre COMMUNE et DCLT

distances[, correction_decay := sum(nbtrajets_par_ind_ij)/sum(nbtrajets_par_ind_ij_decay), 
          by = c("fromidINS", "DCLT")]
distances[, nbtrajets_par_ind_ij_decay := nbtrajets_par_ind_ij_decay * correction_decay]

# nb de trajets par ind par mode
distances[, paste0("nbtraj_ij_", transport) := nbtrajets_par_ind_ij * .SD, .SDcol = paste0("proba_", transport)]

distances[, paste0("nbtraj_ij_d", transport) := nbtrajets_par_ind_ij_decay * .SD, .SDcol = paste0("proba_", transport)]

# sauvegarde
arrow::write_parquet(distances, "/scratch/distances/distances_emplois.parquet")
 
