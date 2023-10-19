
library(qs)
library(data.table)
library(sf)
library(glue)
library(stars)
# remotes::install_github("OFCE/accesstars")
library(accesstars)
library(tidyverse)
library(conflicted)
library(nuvolos)
library(tmap)
library(readxl)
library(archive)
library(vroom)
conflict_prefer_all('dplyr', quiet = TRUE)

load("baselayer.rda") # executer 1. zones avant

# on prend les IRIS 2022
iris <- qread(iris_file)

c200 <- qread(c200_file) # version 2017

Marseille_c200s <- accesstars::idINS2stars(c200 |> select(ind,idINS, IDCOM = com22), zone_emploi)

c200ze <- c200[zone_emploi, ]

iris_ze <- iris[zone_emploi, ] |>
  rename(COM=DEPCOM) |> 
  group_by(COM) |>
  summarize(DEP = first(DEP)) |>
  rename(idcom=COM)

mb_file <- archive_extract(
  "https://www.insee.fr/fr/statistiques/fichier/7637844/RP2020_mobpro_csv.zip",
  "/tmp")
mobpro <- vroom("/tmp/{mb_file[[1]]}" |> glue())

setDT(mobpro)

mobpro <- mobpro[between(as.numeric(AGEREVQ),18,64),]
mobpro <- mobpro[,COMMUNE :=fifelse(ARM=="ZZZZZ", COMMUNE, ARM)]

communes <- qs::qread(communes_file)
communes_emplois <- communes$INSEE_COM
scot <- communes |>
  filter(SIREN_EPCI %in% epci.metropole) |> 
  pull(INSEE_COM)

mobpro[, filter_live := COMMUNE %in% scot] 
mobpro[, filter_work := DCLT %in% communes_emplois]

mobpro <- mobpro[!(filter_live == FALSE & filter_work == FALSE)]

# ---- Synthèse MOBPRO par codes NAF et modes de transport ----
mobilites <- mobpro[, .(NB = sum(IPONDI), 
                        NB_in = sum((COMMUNE%chin%scot)*IPONDI)), 
                    by = c("COMMUNE", "DCLT", "NA5", "TRANS")]
mobilites[, TRANS := factor(TRANS) |> 
            fct_collapse(
              "none" = "1","walk" = "2", "bike" = "3", 
              "car" = c("4", "5"), "transit" = "6")]

emplois_by_DCLT <- mobilites[,
                             .(emp = sum(NB), emp_scot = sum(NB_in)),
                             by = c("DCLT", "NA5")]

# ---- Récupération des surfaces de FF2018 par code NAF ----
con <- nuvolos::get_connection()
locaux <- DBI::dbGetQuery(
  con, 
  "SELECT sprincp, cconac, stoth, slocal, idcom, X, Y
    FROM FF2018_PB0010_LOCAL
    WHERE ccodep IN ('13', '30', '84', '83', '04') AND
     (cconac IS NOT NULL OR stoth > 0) ;")

locaux <- locaux |> 
  filter(!is.na(CCONAC), !is.na(X), !is.na(Y)) |> 
  mutate(NAF = str_sub(CCONAC, 1, 2),
         idINS = r3035::idINS3035(X,Y)) |> 
  rename(sp = SPRINCP,
         sh = STOTH,
         slocal = SLOCAL) |> 
  mutate(ts = ifelse(sp!=0, sp,
                     ifelse(sh!=0, sh/2, slocal/4)))
locaux_h <- locaux 

curl::curl_download(
  "https://www.insee.fr/fr/statistiques/fichier/2120875/naf2008_5_niveaux.xls",
  "/tmp/naf.xls")
naf <- readxl::read_excel("/tmp/naf.xls") |>
  select(NAF=NIV2, NAF1=NIV1) |>
  group_by(NAF) |>
  summarise(NAF1 = dplyr::first(NAF1))

naf <- naf |> mutate(group_naf = case_when(
  NAF1 == "A" ~ "AZ",
  NAF1 == "F" ~ "FZ",
  NAF1 %in% LETTERS[2:5] ~ "BE",
  NAF1 %in% LETTERS[15:17] ~ "OQ",
  NAF1 %in% c(LETTERS[7:14], LETTERS[18:21]) ~ "GU",
  TRUE ~ NA_character_)) |> 
  setDT()

locaux <- naf[locaux, on = "NAF"]

# ---- Rasterisation des surfaces au carreau 200 par code NAF ----

surf_by_naf <- locaux |> 
  filter(ts>0, IDCOM %in% communes_emplois) |> 
  group_by(idINS, group_naf) |> 
  summarize(ts=sum(ts),
            IDCOM = first(IDCOM), .groups="drop")

surf_by_naf.h <- locaux_h |> 
  filter(ts>0, IDCOM %in% communes_emplois) |> 
  group_by(idINS) |> 
  summarize(ts=sum(ts),
            IDCOM = first(IDCOM))

# on inclut les individus pour une petite astuce à venir

ind_ze <- c200ze |> 
  st_drop_geometry() |>
  select(idINS, ind, IDCOM = com22)
  filter(ind>0) 

surf_by_naf <- full_join(surf_by_naf, ind_ze, by="idINS", suffix=c("", ".ind")) |> 
  mutate(ts  = replace_na(ts, 0),
         ind = replace_na(ind, 0),
         IDCOM = if_else(is.na(IDCOM),IDCOM.ind, IDCOM),
         group_naf = replace_na(group_naf, "AZ")) |> 
  select(-IDCOM.ind)

# surf_by_naf <- locaux |>
#   filter(ts > 0, IDCOM %in% communes_emplois) |>
#   drop_na(group_naf, X, Y) |> 
#   select(ts, group_naf, X, Y, IDCOM) |>
#   st_as_sf(coords = c("X", "Y"), crs = 3035)
# 
# # c200ze <- c200.scot_tot |> filter(com %in% com_ze$insee) #pk je n'ai pas de commune??? on pourrait faire avec les polynomes mais c chiant
# 
# template_lr <- Marseille_c200s
# template_lr[[1]][] <- 0
# 
# surf_by_naf.st <- map(
#   surf_by_naf |> select(ts, group_naf) |> group_split(group_naf), \(x) {
#   res <- st_rasterize(x, template = template_lr, options = "MERGE_ALG=ADD")
#   names(res) <- x$group_naf[1]
#   return(res)
# }) |> reduce(c) 

surf_by_naf_h <- surf_by_naf |> 
  group_by(idINS) |> 
  summarize(IDCOM = first(IDCOM),
            ts = sum(ts),
            ind = sum(ind))


template_lr <- Marseille_c200s
template_lr[[1]][] <- 0

surf_by_naf_h.st <- st_rasterize(surf_by_naf_h, 
                                 template = template_lr, 
                                 options = "MERGE_ALG=ADD")

# ---- Surfaces totales par NAF et commune ----
communes.st <- c200 |> 
  transmute(idINS,
            DCLT = str_sub(CODE_IRIS, 1, 5) |> as.integer()) |> # rasterize trompeur sur char
  accesstars::idINS2stars(zone_emploi)

surf_by_naf_DCLT <- c(surf_by_naf.st, communes.st) |> 
  as_tibble() |>
  select(-x, -y) |> 
  pivot_longer(cols = -DCLT, names_to = "NA5", values_to = "ts") |> 
  filter(DCLT != 0, ts != 0) |> 
  group_by(DCLT, NA5) |> 
  summarise(ts = sum(ts), .groups = "drop") |> 
  mutate(DCLT = as.character(DCLT))

# ---- Estimation des emplois en fonction de la surface ----
# ATTENTION : il y a des emplois dans des communes sans surfaces correspondantes.
(verif <- anti_join(emplois_by_DCLT, surf_by_naf_DCLT, by = c("DCLT", "NA5")))
verif[, sum(emp)] / emplois_by_DCLT[, sum(emp)]
verif[, sum(emp_scot)] / emplois_by_DCLT[, sum(emp_scot)]

emplois_by_DCLT_c <- emplois_by_DCLT |> 
  left_join( surf_by_naf_DCLT, by = c("DCLT", "NA5")) |> 
  filter(DCLT %in% communes_emplois) |> 
  group_by(DCLT) |> 
  mutate(zut = all(is.na(ts)),
         orphelins = sum(emp[is.na(ts)]),
         emp_c = emp + orphelins*emp/sum(emp[!is.na(ts)]),
         emp_c = ifelse(is.na(ts)&!zut, 0,
                        ifelse(zut, 0 , emp_c))) |> 
  ungroup() 

emp_pred <- c(surf_by_naf.st, communes.st) |> 
  as_tibble() |>
  pivot_longer(cols = -c(DCLT, x, y), names_to = "NA5", values_to = "ts") |> 
  filter(DCLT != 0, ts != 0) |> 
  inner_join(emplois_by_DCLT_c |> mutate(DCLT=as.numeric(DCLT)), by=c("DCLT", "NA5"), suffix = c("", ".com")) |> 
  mutate(emp_pred = ts/ts.com * emp_c,
         emp_pred_scot = emp_pred/emp*emp_scot) |> 
  select(x,y,emp_pred, emp_pred_scot) |>
  st_as_sf(coords = c("x", "y"), crs=3035) |> 
  st_rasterize(template = template_lr, options = "MERGE_ALG=ADD")

# 
# mod_surf_to_emp <- emplois_by_DCLT_c |>
#   as_tibble() |> 
#   nest(data_secteur = -NA5) |>
#   mutate(lm_res = map(data_secteur, ~ {
#     data <- filter(.x, nb_emplois_c > 0, ts > 0)
#     if(nrow(data)<3) return(NA)
#     lm( log(nb_emplois_c) ~ log(ts), data = data)}))
# 
# input_jobs <- function(x, ...) {
#   if (is.na(x) | x == 0) return(0)
#   (exp(predict(object = ..., newdata = data.frame(ts = x))))
# }
# 
# emp_pred <- imap(mod_surf_to_emp$lm_res, ~ {
#   st_apply(X = surf_by_naf.st[mod_surf_to_emp$NA5[.y]],
#            MARGIN = c("x", "y"),
#            FUN = "input_jobs",
#            object = .x)
# })

emp_pred_h <- as_tibble(c(surf_by_naf_h.st, communes.st)) |> 
  filter(DCLT != 0, ts != 0) |> 
  mutate(DCLT = as.character(DCLT)) |> 
  right_join(emplois_by_DCLT_c |> 
               filter(zut) |>
               group_by(DCLT) |> 
               summarise(nb_emplois=sum(nb_emplois, na.rm=TRUE)), by="DCLT") |> 
  group_by(DCLT) |> 
  mutate(emp_pred = ts/sum(ts)*nb_emplois) |> 
  ungroup() |> 
  select(x,y,emp_pred) |> 
  drop_na(x,y) |>
  st_as_sf(coords=c("x", "y"), crs=3035) |> 
  st_rasterize(template = template_lr, options = "MERGE_ALG=ADD")

names(emp_pred_h) <- "h" 
les_emplois <- c(emp_pred, emp_pred_h) |> 
  mutate(emp_tot = emp_pred + h)
les_emplois <- c(les_emplois, communes.st)

# ---- Estimation des marges des emplois (uniquement) pourvus par les résidents ----
# NB : avec MOBPRO, les emplois pris en compte sont déjà ceux uniquement des résidents
les_emplois <- les_emplois  |> 
  as_tibble() |> 
  filter(emp_tot > 0, DCLT !=0) |> 
  mutate(idINS = coord2idINS(x, y)) |> 
  select(-x, -y) 
emp_mobpro <- mobpro[filter_live==TRUE&filter_work==TRUE, .(emp_r = sum(IPONDI)), by="DCLT"]
les_emplois <- les_emplois |> 
  mutate(DCLT = as.character(DCLT)) |> 
  select(emp_tot, idINS, DCLT) |> 
  inner_join(emp_mobpro, by="DCLT") |> 
  group_by(DCLT) |> 
  mutate(emp_des_res = emp_tot*first(emp_r)/sum(emp_tot))

# ---- Calcul mobilité professionnelle par NA5 tout mode ----
mobilites <- dcast(mobilites, COMMUNE + DCLT + NA5 ~ TRANS, 
                   fun.aggregate = sum, 
                   value.var = "NB") 

communes_residents <- scot_tot.epci

mobilites[, DCLT := fifelse(as.character(DCLT) %in% communes_emplois, as.character(DCLT), "Hors zone")]
mobilites[, COMMUNE := fifelse(as.character(COMMUNE) %in% communes_residents, as.character(COMMUNE), "Hors zone")]

mobilites <- mobilites[COMMUNE != "Hors zone"] # ici on n'a pas besoin des travailleurs non résidents.

mobilites[, tot_navetteurs := walk + bike + transit + car]

# ---- Estimation des marges des actifs résidents ----
popact <- readxl::read_xlsx("~/files/base-cc-emploi-pop-active-2018.xlsx" |> glue(), sheet = "COM_2018", skip=5) |>
  transmute(COMMUNE=CODGEO,
            pop1564 = P18_POP1564,
            act1564 = P18_ACT1564,
            chom1564 = P18_CHOM1564) 

les_actifs <- c200ze |> 
  mutate(COMMUNE = str_sub(IRIS, 1, 5)) |> 
  filter(COMMUNE %in% communes_residents) |> 
  left_join(popact, by = "COMMUNE") |> 
  transmute(COMMUNE, 
            act_18_64 = ind_18_64 * (act1564-chom1564)/pop1564,
            fromidINS = idINS) |> 
  st_drop_geometry()

# ---- Estimation des marges des fuyars résidents ----
les_fuites <- mobilites[, .(fuite = sum(tot_navetteurs * (DCLT == "Hors zone") / sum(tot_navetteurs))),
                        by = "COMMUNE"]

les_actifs <- les_actifs |> inner_join(les_fuites, by = "COMMUNE")
