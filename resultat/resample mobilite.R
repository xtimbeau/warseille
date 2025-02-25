# On part des enquêtes mobilités, on le sresample pour calculer les IC à 95%
library(tidyverse)
library(ofce)

deploc_individu <- bd_read("deploc_individu")
source("deploc/f.deploc_stat.r")

aa4 <- deploc_densite_2(
  deploc_individu |> filter(TAA2017_RES==4), label_reg="Aire attraction plus de 700k (hors Paris)",
  var = "DENSITECOM_RES", 
  mode = "car",
  soustitre = ", en voiture",
  labels = c("très dense", "densité intermédiaire", "peu dense", "très peu dense"))
EMP2019_AA4 <- aa4$raw
bd_write(EMP2019_AA4)