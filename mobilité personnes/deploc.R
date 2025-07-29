library(tidyverse)
library(glue)
library(qs)
library(conflicted)

conflict_prefer_all("dplyr", quiet=TRUE)

load("baselayer.rda")

source(enqmobstarter_file)

les_descripteurs <- c(
  "IDENT_MEN", "MDATE_jour", "MDATE_mois", "TYPEJOUR", "VAC_SCOL",
  "SEXE", "AGE", "CS_ACT", "ACTOCCUP", "CS24", "TYPMEN5", "decile_rev_uc",
  "quartile_rev_uc", "npers", "nenfants", "mode",
  "TUU2017_RES", "STATUTCOM_UU_RES", "TYPE_UU_RES", "TAA2017_RES", 
  "CATCOM_AA_RES", "DENSITECOM_RES", "nb_voitures", "dist_tc",
  "POND_JOUR", "pond_indC", "pond_menC")

# Une ligne par individu-motif-boucle
stat_bouc <- deploc |> 
  filter(mobloc == 1) |> 
  group_by(IDENT_IND, motif_principal, boucle) |> 
  summarise(nb_trajets = n(),
            distance = sum(MDISTTOT_fin),
            duree = sum(DUREE),
            across(.cols = all_of(les_descripteurs), first),
            .groups = "drop_last") |>
  group_by(IDENT_IND) |> 
  mutate(distance = distance * POND_JOUR,
         duree = duree * POND_JOUR) |> 
  summarise(across(all_of(les_descripteurs), first),
            motif_principal = first(motif_principal),
            distance = sum(distance, na.rm = TRUE)/sum(POND_JOUR, na.rm=TRUE),
            duree = sum(duree, na.rm = TRUE)/sum(POND_JOUR),
            nb_boucle = n()) |> 
  mutate(
    nb_voitures = factor(replace_na(nb_voitures, 0)),
    voiture = factor(nb_voitures !=0),
    dist_tc = factor(dist_tc, ordered=FALSE),
    quartile_rev_uc = factor(quartile_rev_uc),
    TAA2017_RES = factor(TAA2017_RES),
    TUU2017_RES = factor(TUU2017_RES),
    nb_voitures = factor(nb_voitures),
    DENSITECOM_RES = factor(DENSITECOM_RES),
    dist_tc = factor(dist_tc),
    TYPMEN5 = factor(TYPMEN5), 
    motif_principal = factor(motif_principal), 
    typejour = factor(TYPEJOUR),
    vacscol = factor(VAC_SCOL), 
    pond_jour = POND_JOUR) |> 
  ungroup()

KVKM1ANV <- deploc |>
  unnest(vehicules) |> 
  group_by(IDENT_MEN) |>
  summarize(KVKM1ANV=sum(KVKM1ANV)/n_distinct(IDENT_DEP))

stat_bouc <- stat_bouc |> 
  left_join(KVKM1ANV, by="IDENT_MEN")

qs::qsave(stat_bouc, "mod/data/stat_bouc.qs")