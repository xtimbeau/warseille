
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

 curl::curl_download(
  "https://www.insee.fr/fr/statistiques/fichier/7637844/RP2020_mobpro_csv.zip",
  "/tmp/mobpro.zip")
mb_file <- archive_extract("/tmp/mobpro.zip", "/tmp")
mobpro <- vroom("/tmp/{mb_file[[1]]}" |> glue())

setDT(mobpro)

mobpro <- mobpro[between(as.numeric(AGEREVQ),18,64),]
mobpro <- mobpro[,COMMUNE :=fifelse(ARM=="ZZZZZ", COMMUNE, ARM)]

scot <- qs::qread(communes_ar_file) |>
  filter(SIREN_EPCI %in% epci.metropole) |> 
  pull(INSEE_COM) |> sort()

mobpro[, filter_live := COMMUNE %in% scot] 

mobpro <- mobpro[(filter_live == TRUE) , ]

# ---- Synthèse MOBPRO par codes NAF et modes de transport ----

mobilites <- mobpro[, .(NB = sum(IPONDI), 
                        NB_in = sum((COMMUNE%chin%scot)*IPONDI),
                        fl = first(filter_live)), 
                    by = c("COMMUNE", "DCLT", "NA5", "TRANS")]

mobilites[, TRANS := factor(TRANS) |> 
            fct_collapse(
              "none" = "1","walk" = "2", "bike" = "3", 
              "car" = c("4", "5"), "transit" = "6")]

mobpro <- mobilites 
communes <- qs::qread(communes_ar_file)

coms <- communes |>
  select(insee = INSEE_COM) |> 
  st_centroid() |> 
  as_tibble()

mobpro99 <- mobilites |> 
  st_drop_geometry() |> 
  group_by(COMMUNE, DCLT) |> 
  summarize(fl = any(fl),
            emp = sum(NB),
            emp_scot = sum(NB_in), .groups = "drop") |> 
  left_join(coms , by = c("COMMUNE"="insee"), suffix = c("", ".o")) |> 
  left_join(coms, by = c("DCLT"="insee"), suffix = c("", ".d") ) |> 
  mutate(d = st_distance(geometry, geometry.d, by_element = TRUE)) |> 
  arrange(d) |> 
  mutate(cemp = cumsum(emp_scot)/sum(emp_scot),
         mobpro99 = cemp <= .99,
         mobpro95 = cemp <= .95) |> 
  transmute(COMMUNE, DCLT, fl, mobpro99, mobpro95, d, cemp)

mobilites <- mobilites |> 
  left_join(mobpro99 |> select(COMMUNE, DCLT, mobpro99, mobpro95), by=c("COMMUNE", "DCLT")) 

qs::qsave(mobilites, mobpro_file)
communes_emplois <- mobpro99 |> filter(mobpro95) |> distinct(DCLT)
communes95 <- mobpro99 |> filter(mobpro95) |> distinct(COMMUNE, DCLT)
communes95 <- unique(c(communes95$COMMUNE, communes95$DCLT))
communes <- communes |>
  filter(INSEE_COM %in% communes95)

qs::qsave(communes, communes_mb99_file)  

emplois_by_DCLT <- mobilites[ mobpro95==TRUE,
                             .(emp = sum(NB), emp_scot = sum(NB_in)),
                             by = c("DCLT", "NA5")]


c200ze <- qread(c200_file) |> 
  filter(com22 %in% communes95)

# ---- Récupération des surfaces de FF2018 par code NAF ----
con <- nuvolos::get_connection()

deps <- communes |> 
  distinct(INSEE_DEP) |>
  pull() |> 
  str_c(collapse = "','") |> 
  sort()
deps <- str_c("('", deps , "')")

locaux <- DBI::dbGetQuery(
  con, 
  glue("SELECT sprincp, cconac, stoth, slocal, idcom, X, Y
    FROM FF2018_PB0010_LOCAL
    WHERE ccodep IN {deps} AND
     (cconac IS NOT NULL OR stoth > 0) ;"))

locaux <- locaux |> 
  filter(IDCOM%in%communes95) |> 
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

locaux <- locaux |> 
  left_join(naf, by = "NAF")

# ---- Rasterisation des surfaces au carreau 200 par code NAF ----

surf_by_naf <- locaux |> 
  filter(ts>0) |> 
  group_by(idINS, group_naf) |> 
  summarize(ts=sum(ts),
            IDCOM = first(IDCOM), .groups="drop")

surf_by_naf.h <- locaux_h |> 
  filter(ts>0) |> 
  group_by(idINS) |> 
  summarize(ts=sum(ts),
            IDCOM = first(IDCOM))

# on inclut les individus pour une petite astuce à venir

ind_ze <- c200ze |> 
  st_drop_geometry() |>
  select(idINS, ind, IDCOM = com22)

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

surf_by_naf_DCLT <- surf_by_naf |> 
  rename(DCLT = IDCOM, NA5 = group_naf) |> 
  group_by(DCLT, NA5) |> 
  summarize(ts = sum(ts),
            ind = sum(ind),
            .groups = "drop")

# ---- Estimation des emplois en fonction de la surface ----
# ATTENTION : il y a des emplois dans des communes sans surfaces correspondantes.
# pour les communes dans le périmètre, on utilise le nombre d'individus plutôt que les surfaces
# (pas de surface pour le code NAF)
# Il reste alors les communes hors périmètre (il y a un flux du scot vers ces communes)
# mais elles sont au delà de 33km
# ca fait un flux de 1% qui va être la fuite

(verif <- anti_join(as_tibble(emplois_by_DCLT), surf_by_naf_DCLT |> filter(ts>0), by = c("DCLT", "NA5")))
complement <- ind_ze |> 
  rename(DCLT = IDCOM) |> 
  inner_join(verif, by=c("DCLT"), relationship = "many-to-many") |> 
  mutate(ts = ind)

surf_by_naf_c <- surf_by_naf |>
  filter(ts>0) |> 
  bind_rows(complement |> select(idINS, IDCOM = DCLT, group_naf = NA5, ts, ind))
surf_by_naf_c_DCLT <- surf_by_naf_c |> 
  rename(DCLT = IDCOM, NA5 = group_naf) |> 
  group_by(DCLT, NA5) |> 
  summarize(ts = sum(ts),
            ind = sum(ind),
            .groups = "drop")
(verif2 <- anti_join(as_tibble(emplois_by_DCLT), surf_by_naf_c_DCLT, by = c("DCLT", "NA5")))

sum(verif2$emp) / emplois_by_DCLT[, sum(emp)]
sum(verif2$emp_scot) / emplois_by_DCLT[, sum(emp_scot)]

emplois_by_DCLT_c <- emplois_by_DCLT |> 
  left_join( surf_by_naf_c_DCLT, by = c("DCLT", "NA5")) |> 
  filter(DCLT %in% communes_emplois$DCLT) |> 
  ungroup() 

surf_by_naf <- surf_by_naf_c

emp_pred <- surf_by_naf |>
  rename(DCLT = IDCOM, 
         NA5 = group_naf) |> 
  select(-ind) |> 
  filter(ts > 0) |> 
  inner_join(emplois_by_DCLT_c, by=c("DCLT", "NA5"), suffix = c("", ".com")) |> 
  mutate(emp_pred = ts/ts.com * emp,
         emp_pred_scot = ts/ts.com *emp_scot) |> 
  select(emp_pred, emp_pred_scot, idINS, NA5, DCLT) |> 
  group_by(idINS) |> 
  summarize(
    DCLT = first(DCLT),
    emp_pred = sum(emp_pred, na.rm=TRUE),
    emp_pred_scot = sum(emp_pred_scot, na.rm=TRUE))

qs::qsave(emp_pred, emp_pred_file)
rm(c200, communes)
gc()
# on génére ensuite les paires d'idINS pertinentes (ie associées à un flux mobpro)

com_paires <- mobilites |> 
  filter(fl, mobpro95) |> 
  rename(mode = TRANS) |> 
  group_by(COMMUNE, DCLT, mode) |> 
  summarize(emp_scot = sum(NB_in), .groups = "drop") |> 
  pivot_wider(id_cols = c(COMMUNE, DCLT), names_from = mode, values_from = c(emp_scot), values_fill = 0) |> 
  transmute(COMMUNE, DCLT, across(c(car, walk, bike, transit), ~ .x>0))

c200ze <- c200ze |> 
  st_drop_geometry() 

paires_mobpro <- pmap_dfr(com_paires, ~{
  from <- c200ze |> 
    filter(com22==..1, ind>0) |>
    select(fromidINS = idINS)
  if(nrow(from)>0) {
    from_4326 <- r3035::idINS2lonlat(from$fromidINS) |> 
      rename(o_lon = lon, o_lat = lat)
    from <- from |> 
      bind_cols(from_4326)
  }
  to <- emp_pred |>
    filter(DCLT==..2, emp_pred>0) |> 
    select(toidINS = idINS)
  if(nrow(to)>0) {
    to_4326 <- r3035::idINS2lonlat(to$toidINS) |> 
      rename(d_lon = lon, d_lat = lat)
    to <- to |> 
      bind_cols(to_4326)
  }
  if(length(to)>0&length(from)>0) {
    it <- dplyr::cross_join(from, to)
    it <- it |> 
      dplyr::mutate(COMMUNE = .x, DCLT = .y, car = ..3, walk = ..4, bike = ..5, transit = ..6,
             euc = r3035::idINS2dist(fromidINS, toidINS)/1000) 
  } else
    it <- tibble::tibble()
  it
}) 
unlink(paires_mobpro_dataset)
arrow::write_dataset(paires_mobpro, paires_mobpro_dataset, partitioning = "COMMUNE")

# je pense qu'on a pas besoin de la suite, ce qui compte c'est emp_pred
# en bonus on a emp_pred_scot
# # ---- Estimation des marges des emplois (uniquement) pourvus par les résidents ----
# # NB : avec MOBPRO, les emplois pris en compte sont déjà ceux uniquement des résidents
# les_emplois <- les_emplois  |> 
#   as_tibble() |> 
#   filter(emp_tot > 0, DCLT !=0) |> 
#   mutate(idINS = coord2idINS(x, y)) |> 
#   select(-x, -y) 
# emp_mobpro <- mobpro[filter_live==TRUE&filter_work==TRUE, .(emp_r = sum(IPONDI)), by="DCLT"]
# les_emplois <- les_emplois |> 
#   mutate(DCLT = as.character(DCLT)) |> 
#   select(emp_tot, idINS, DCLT) |> 
#   inner_join(emp_mobpro, by="DCLT") |> 
#   group_by(DCLT) |> 
#   mutate(emp_des_res = emp_tot*first(emp_r)/sum(emp_tot))
# 
# # ---- Calcul mobilité professionnelle par NA5 tout mode ----
# mobilites <- dcast(mobilites, COMMUNE + DCLT + NA5 ~ TRANS, 
#                    fun.aggregate = sum, 
#                    value.var = "NB") 
# 
# communes_residents <- scot_tot.epci
# 
# mobilites[, DCLT := fifelse(as.character(DCLT) %in% communes_emplois, as.character(DCLT), "Hors zone")]
# mobilites[, COMMUNE := fifelse(as.character(COMMUNE) %in% communes_residents, as.character(COMMUNE), "Hors zone")]
# 
# mobilites <- mobilites[COMMUNE != "Hors zone"] # ici on n'a pas besoin des travailleurs non résidents.
# 
# mobilites[, tot_navetteurs := walk + bike + transit + car]
# 
# # ---- Estimation des marges des actifs résidents ----
# popact <- readxl::read_xlsx("~/files/base-cc-emploi-pop-active-2018.xlsx" |> glue(), sheet = "COM_2018", skip=5) |>
#   transmute(COMMUNE=CODGEO,
#             pop1564 = P18_POP1564,
#             act1564 = P18_ACT1564,
#             chom1564 = P18_CHOM1564) 
# 
# les_actifs <- c200ze |> 
#   mutate(COMMUNE = str_sub(IRIS, 1, 5)) |> 
#   filter(COMMUNE %in% communes_residents) |> 
#   left_join(popact, by = "COMMUNE") |> 
#   transmute(COMMUNE, 
#             act_18_64 = ind_18_64 * (act1564-chom1564)/pop1564,
#             fromidINS = idINS) |> 
#   st_drop_geometry()
# 
# # ---- Estimation des marges des fuyars résidents ----
# les_fuites <- mobilites[, .(fuite = sum(tot_navetteurs * (DCLT == "Hors zone") / sum(tot_navetteurs))),
#                         by = "COMMUNE"]
# 
# les_actifs <- les_actifs |> inner_join(les_fuites, by = "COMMUNE")
