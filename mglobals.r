# init ---------------
library(tidyverse)
library(glue)
library(conflicted)
library(rlang)
library(glue)
library(stringr)
library(pins)
library(AzureStor) 

# globals -----------------------------------------------------------------
ville <- "Marseille"
epci.metropole <- "200054807"

# fichiers importants
mdir <- "/space_mounts/data/marseille/"
localdata <- "~/files/localr5/" |> glue()
scripts <- "~/marseille"
mob2019 <- "{mdir}/mob2019/" |> glue()
home <- "~/marseille"
temp_dir <- "/tmp"
r5files_rep <- "{mdir}/distances/src/" |> glue()
newpredict_file <- "annexes/newpredict.r"
enqmobstarter_file <- "enqmob_starter_V2.R"
mod_rep <- "{mdir}/mod" |> glue()
output_rep <- "{mdir}/output/" |> glue()
dir.create(output_rep)
dir.create(mod_rep)

# distances, probas et tutti quanti
r5files_rep <- "{mdir}/distances/router" |> glue()
r5_output <- set_names(
  c("bike.parquet", "cardgr.parquet", "transit.parquet", "walk.parquet"),
  c("bike",         "car",          "transit",         "walk"))
dir_dist <- "{mdir}/distances" |> glue()
idINS_file <- "{mdir}/idINS.parquet" |> glue()
# idINS_scol_file <- "{repository_distances_scol}/idINS.parquet" |> glue()
distances_file <- "{dir_dist}/distances.parquet" |> glue()
dist_dts <- "{dir_dist}/src/distances_dataset" |> glue()
proproba_file <- "{dir_dist}/proba_pro.parquet" |> glue()
nonproproba_file <- "{dir_dist}/proba_nonpro.parquet"
kmpro_file <- "{dir_dist}/km_pro.parquet"
kmnonpro_file <- "{dir_dist}/km_nonpro.parquet"
trajets_pro_file <- "{dir_dist}/trajets_pro.parquet" |> glue()
meaps_rep <- "{dir_dist}/meaps" |> glue()
dir.create(meaps_rep)
meaps_file <- "{meaps_rep}/meaps_est.rda" |> glue()
decor_carte_file <- "{mdir}/decor_carte.rda" |> glue()
mobilites_file <- "{mdir}/mobilites.qs" |> glue()
repository_notc <- "{temp_dir}/notc" |> glue()
dir.create(repository_notc)
localr5 <- str_c(localdata, "/r5_base")
localr5car <- str_c(localdata, "/r5car")

trg_file <- "{dir_dist}/trg.qs" |> glue()
drg_file <- "{dir_dist}/drg.qs" |> glue()
trgc_file <- "{dir_dist}/trgc.qs" |> glue()
drgc_file <- "{dir_dist}/drgc.qs" |> glue()
time_dts <- "{dir_dist}/time_dataset" |> glue()
time_matrix <- "{dir_dist}/time_matrix.qs" |> glue()
rank_matrix <- "{dir_dist}/rank_matrix.qs" |> glue()
delta_dts <- "{mdir}/delta_iris" |> glue()
# résultats intermédiaires
mobpro_file <- "{mdir}/mobpro.qs" |> glue()
empetact <- "{mdir}/empetact.qs" |> glue()
paires_mobpro_dataset <- "{mdir}/paires_mobpro" |> glue()

## informations spécifique sur la ville
elevation <- "{localr5}/elev_aix_marseille.tif" |> glue()
empze_file <- "{mdir}/empze.qs" |> glue()
c200_file <- "{mdir}/c200_17.qs" |> glue()
c200ze_file <- "{mdir}/c200ze.qs" |> glue()
pbf_file <- "lr.pbf"
pbf_rep <- "{mdir}/OSM/" |> glue()
communes_file <- "{mdir}/communes.qs" |> glue()
communes_ref_file <- "{mdir}/communes_ref.qs" |> glue()
communes_ar_file <- "{mdir}/communes_ar.qs" |> glue()
communes_mb99_file <- "{mdir}/communes_mb99.qs" |> glue()
emp_pred_file <- "{mdir}/emp_pred.qs" |> glue()

classification_urbaine <- "{localdata}/autres/classification_urbaine.csv" |> glue()
iris_file <- glue("{mdir}/iris.qs")
com2021_file <- glue("{mdir}/com2021.qs")
com2017_file <- glue("{mdir}/com2017.qs")

## enquetes nationales
deploc_file <- "{mob2019}/k_deploc_public.csv" |> glue()
k_individu_file <- "{mob2019}/k_individu_public.csv" |> glue()
tcm_men_file <- "{mob2019}/tcm_men_public.csv" |> glue()
q_men_file <- "{mob2019}/q_menage_public.csv" |> glue()
tcm_ind_kish_file <- "{mob2019}/tcm_ind_kish_public.csv" |> glue()
enqmobpro <- "{mdir}/mobpro2018.csv" |> glue()
#enqmobscol <- "{DVFdata}/sources/MOB/mobscol2018.csv" |> glue()

# c200ze_file <- marseille_board  %>% pin_download(c200ze.qs) |> glue()

# 100 minutes de trajet en voiture max et
# 35km pour les probas (au delà proba_car=1)
seuil_temps_car <- 120
seuil_distance_proba <- 35000

source("secrets/azure.R")
progressr::handlers(global=TRUE)
progressr::handlers("cli")