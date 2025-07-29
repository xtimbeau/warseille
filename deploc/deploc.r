library(tidyverse)
library(glue)
library(qs)
library(conflicted)
conflict_prefer_all("dplyr", quiet=TRUE)

deploc <- bd_read("deploc")

les_descripteurs <- c(
  "IDENT_MEN", "MDATE_jour", "MDATE_mois", "TYPEJOUR", "VAC_SCOL",
  "SEXE", "AGE", "CS_ACT", "ACTOCCUP", "CS24", "TYPMEN5", "decile_rev_uc",
  "quartile_rev_uc", "revuce", "npers", "nenfants", "mode",
  "TUU2017_RES", "STATUTCOM_UU_RES", "TYPE_UU_RES", "TAA2017_RES", 
  "CATCOM_AA_RES", "DENSITECOM_RES",
  "TUU2017_DES", "STATUTCOM_UU_DES", "TYPE_UU_DES", "TAA2017_DES", 
  "CATCOM_AA_DES", "DENSITECOM_DES", "REG_ORI", "NUTS_ORI",
  "saison", "classement_montagne_ori", "classement_littoral_ori",
  "nb_voitures", "dist_tc", "n_trajinboucle",
  "POND_JOUR", "pond_indC", "pond_menC")

# Une ligne par individu-motif-boucles
boucles <- deploc |> 
  filter(mobloc == 1) |> 
  filter(n_trajinboucle!=1) |> 
  group_by(IDENT_IND, motif_principal, boucle) |> 
  summarise(nb_trajets = n(),
            distance = sum(MDISTTOT_fin),
            duree = sum(DUREE),
            bcl = first(n_trajinboucle)-1,
            across(.cols = all_of(les_descripteurs), first),
            .groups = "drop") 
# une ligne par individu motif
stat_bouc <- boucles |>
  group_by(IDENT_IND, motif_principal) |> 
  mutate(distance = distance * POND_JOUR,
         duree = duree * POND_JOUR,
         bcl_simple = n_trajinboucle==2) |> 
  summarise(
    across(all_of(les_descripteurs), first),
    distance = sum(distance, na.rm = TRUE)/sum(POND_JOUR, na.rm=TRUE),
    distance_bcls = sum(distance*bcl_simple, na.rm = TRUE)/sum(POND_JOUR*bcl_simple, na.rm=TRUE),
    duree = sum(duree, na.rm = TRUE)/sum(POND_JOUR),
    nb_boucle = n(),
    nb_bcls = sum(bcl_simple, na.rm=TRUE)) |> 
  mutate(
    nb_voitures = factor(replace_na(nb_voitures, 0)),
    voiture = factor(nb_voitures !=0),
    dist_tc = factor(dist_tc, ordered=FALSE),
    quartile_rev_uc = factor(quartile_rev_uc),
    nb_voitures = factor(nb_voitures),
    TAA2017_RES = factor(TAA2017_RES),
    TUU2017_RES = factor(TUU2017_RES),
    DENSITECOM_RES = factor(DENSITECOM_RES),
    TAA2017_DES = factor(TAA2017_DES),
    TUU2017_DES = factor(TUU2017_DES),
    DENSITECOM_DES = factor(DENSITECOM_DES),
    dist_tc = factor(dist_tc),
    TYPMEN5 = factor(TYPMEN5), 
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

return(list(boucles=boucles, stat_bouc = stat_bouc))
