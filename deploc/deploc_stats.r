#on essaye de reproduire les tableaux du SDES
library(tidyverse)
library(ofce)
library(conflicted)
library(here)
library(knitr)
source("mglobals.r")
conflict_prefer_all("dplyr", quiet=TRUE)
future::plan("multisession", workers = 8)

stat_bouc <- source_data("deploc/deploc.r")
deploc_individu <-bd_read("deploc_individu")
source("deploc/f.deploc_stat.r")

aa4 <- deploc_densite_2(
  deploc_individu |> filter(TAA2017_RES==4), label_reg="Aire attraction plus de 700k (hors Paris)",
  var = "DENSITECOM_RES", 
  labels = c("très dense", "densité intermédiaire", "peu dense", "très peu dense"))
EMP2019_AA4 <- aa4$data
bd_write(EMP2019_AA4)
aa4$table[[1]]
