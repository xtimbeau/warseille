library(qs)
library(data.table)
library(sf)
library(glue)
library(stars)
# remotes::install_github("OFCE/accesstars")
# library(accesstars)
library(tidyverse)
library(conflicted)
library(nuvolos)
library(tmap)
library(readxl)
library(archive)
library(vroom)
library(arrow)

conflict_prefer_all('dplyr', quiet = TRUE)

load("baselayer.rda") # executer 1. zones avant

# on prend les IRIS 2022
iris <- qread(iris_file)

c200 <- qread(c200_file) # version 2017

com_ze <- iris[zone_emploi, ] |>
  rename(COM=DEPCOM) |> 
  group_by(COM) |>
  summarize(DEP = first(DEP)) |>
  rename(idcom=COM)

c200ze <- c200 |> 
  semi_join(com_ze |> st_drop_geometry(), by=c("com22"="idcom"))

curl::curl_download(
  "https://www.insee.fr/fr/statistiques/fichier/7637844/RP2020_mobpro_csv.zip",
  "/tmp/mobpro20.zip")
mb_file <- archive_extract("/tmp/mobpro20.zip", dir = "/tmp")
mobpro <- vroom("/tmp/{mb_file[[1]]}" |> glue())

setDT(mobpro)

mobpro <- mobpro[between(as.numeric(AGEREVQ),18,64),]
mobpro <- mobpro[,COMMUNE :=fifelse(ARM=="ZZZZZ", COMMUNE, ARM)]

scot <- qs::qread(communes_ref_file) |>
  filter(ar) |> 
  filter(SIREN_EPCI %in% epci.metropole) |> 
  pull(INSEE_COM) |> sort()

mobpro[, filter_live := COMMUNE %in% scot] 

# mobpro <- mobpro[(filter_live == TRUE) , ]

# ---- Synthèse MOBPRO par codes NAF et modes de transport ----

mobilites <- mobpro[, .(NB = sum(IPONDI), 
                        NB_in = sum(filter_live*IPONDI),
                        fl = any(filter_live)), 
                    by = c("COMMUNE", "DCLT", "NA5", "TRANS")]

mobilites[, TRANS := factor(TRANS) |> 
            fct_collapse(
              "none" = "1","walk" = "2", "bike" = "3", 
              "car" = c("4", "5"), "transit" = "6")]

communes <- qs::qread(communes_ar_file) |> 
  filter(ar)

coms <- communes |>
  select(insee = INSEE_COM) |> 
  st_centroid() |> 
  as_tibble()

mobpro99 <- mobilites |> 
  group_by(DCLT) |> 
  mutate(emp.tot = sum(NB)) |> 
  group_by(COMMUNE, DCLT) |> 
  summarize(fl = any(fl),
            emp = sum(NB),
            emp.tot = first(emp.tot),
            .groups = "drop") |> 
  filter(fl) |> 
  left_join(coms , by = c("COMMUNE"="insee"), suffix = c("", ".o")) |> 
  left_join(coms, by = c("DCLT"="insee"), suffix = c("", ".d") ) |> 
  mutate(d = st_distance(geometry, geometry.d, by_element = TRUE)) |> 
  arrange(d) |> 
  mutate(cemp = cumsum(emp)/sum(emp),
         mobpro99 = cemp <= .99,
         mobpro95 = cemp <= .95) |> 
  transmute(COMMUNE, DCLT, fl, mobpro99, mobpro95, d, cemp, emp, emp.tot)

dclt95 <- mobpro99 |> filter(mobpro95) |> distinct(DCLT) |> pull()
communes <- communes |>
  filter(INSEE_COM %in% scot | INSEE_COM %in% dclt95)

mobilites95 <- mobilites |>
  filter(DCLT %in% dclt95 | COMMUNE %in% scot) |> 
  mutate(mobpro95 = (COMMUNE %in% scot) & (DCLT %in% dclt95))

qs::qsave(mobilites95, mobpro_file)
qs::qsave(communes, communes_mb99_file)  

emplois_by_DCLT <- mobilites95[DCLT %in% dclt95,
                               .(emp = sum(NB), emp_scot = sum(NB[COMMUNE%chin%scot])),
                               by = c("DCLT", "NA5")]


c200ze <- qread(c200_file) |> 
  filter(com22 %in% dclt95)

# ---- Récupération des surfaces de FF2022 par code NAF ----

# ajouter rfp22 pour OQ

# con <- nuvolos::get_connection()

deps <- unique(str_sub(dclt95, 1, 2))

locaux.duck <- open_dataset("/dropbox/dv3f/ff2022/pb0010_local") |> 
  to_duckdb() |> 
  filter(ccodep %in% deps) |> 
  select(sprincp, cconac, stoth, slocal, idcom, X, Y)

locaux <- locaux.duck |> 
  filter(!is.na(cconac), !is.na(X), !is.na(Y)) |> 
  collect() |> 
  mutate(NAF = str_sub(cconac, 1, 2),
         idINS = r3035::sidINS3035(X,Y)) |> 
  rename(sp = sprincp,
         sh = stoth,
         slocal = slocal) |> 
  mutate(ts = sp)

# RFP 2022

rfp <- map_dfr(deps, ~{
  st_read(glue("/dropbox/dv3f/rfp22/rfp_d{.x}.gpkg"), layer="tup")
}) |> st_transform(3035)
rfp_r <- rfp |>
  filter(sprincp>0) |> 
  st_transform(3035) |> 
  transmute(dens = as.numeric(sprincp/st_area(geom)), sprincp)

r200 <- st_as_stars(st_bbox(c200ze), dx=200, dy=200)
pubsuf <-  st_rasterize(rfp_r, template = r200, options = c("MERGE_ALG=ADD")) |> 
  as_tibble() |>
  filter(dens>0) |> 
  mutate(
    tsstar = 200*200*dens,
    group_naf = "OQ",
    idINS = r3035::sidINS3035(x, y)) |> 
  select(-x,-y, dens, sprincp, tsstar) |> 
  mutate(geometry = r3035::sidINS2square(idINS)) |> 
  st_as_sf(crs=3035)

# on rajoute les idcom/iris
iris <- qread(iris_file) |> 
  filter(DEPCOM %in% unique(c200ze$com22)) |> 
  mutate(id = 1:n())

coms <- c200ze |> 
  st_drop_geometry() |> 
  distinct(com22, dep) |> 
  rename(idcom=com22, DEP=dep) 

# empze <- qs::qread(emp_pred_file) |> 
#   rename(emp = emp_pred,
#          emp_resident = emp_pred_scot) |> 
#   mutate(geometry = r3035::idINS2square(idINS)) |> 
#   st_as_sf(crs=3035)

irises <- sf::st_intersects(pubsuf, iris) 
names(irises) <- 1:length(irises)
l_irises  <- map_int(irises, length)
nas <- sf::st_nearest_feature(pubsuf |> filter(l_irises==0), iris)
irises[l_irises==0] <- nas

l2 <- st_join(
  pubsuf |> filter(l_irises>=2), 
  iris |> select(id), largest=TRUE)

irises[l_irises>=2] <- l2 |> pull(id)
flat_irises <- purrr::list_c(irises)

pubsuf <- pubsuf |>
  st_drop_geometry() |> 
  transmute(
    idINS, 
    group_naf,
    dep=iris$DEP[flat_irises],
    idcom=iris$DEPCOM[flat_irises], # on prend les iris pour être cohérent, un carreau peut être sur plusieurs communes
    IRIS = iris$CODE_IRIS[flat_irises],
    tsstar, dens
  ) 

rfp_locaux <- rfp |>
  filter(sprincp>0) |> 
  st_centroid() |> 
  st_transform(3035) |> 
  transmute(
    X = st_coordinates(geom)[,1],
    Y = st_coordinates(geom)[,2], 
    idINS = r3035::sidINS3035(X, Y),
    cconac = "84xxx", NAF = "84",
    sh=0,
    sprincp,
    slocal,
    idcom,
    ts = sprincp) |> 
  st_drop_geometry() |> 
  drop_na(X, Y)

# ifelse(sp!=0, sp,
#           ifelse(sh!=0, sh/2, slocal/4)))

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
  TRUE ~ NA_character_)) 

locaux <- locaux |> 
  left_join(naf, by = "NAF")

# ---- Rasterisation des surfaces au carreau 200 par code NAF ----

surf_by_naf <- locaux |> 
  filter(ts>0) |> 
  group_by(idINS, group_naf) |> 
  summarize(ts=sum(ts),
            idcom = first(idcom), .groups="drop") |> 
  full_join(pubsuf |> select(idINS, group_naf, com=idcom, tsstar), by=c("group_naf", "idINS")) |> 
  mutate( 
    idcom = ifelse(is.na(idcom), com, idcom),
    ts = ifelse(is.na(ts), 0, ts),
    tsstar = ifelse(is.na(tsstar), 0, tsstar),
    tsfull = ts + tsstar) |> 
  select(-tsstar, -com)

# surf_by_naf.h <- locaux_h |> 
#   filter(ts>0) |> 
#   group_by(idINS) |> 
#   summarize(ts=sum(ts),
#             IDCOM = first(IDCOM))

# on inclut les individus pour une petite astuce à venir

ind_ze <- c200ze |>
  st_drop_geometry() |>
  select(idINS, ind, idcom = com22)

surf_by_naf <- full_join(surf_by_naf, ind_ze, by="idINS", suffix=c("", ".ind")) |> 
  mutate(tsfull  = replace_na(tsfull, 0),
         ind = replace_na(ind, 0),
         idcom = if_else(is.na(idcom),idcom.ind, idcom),
         group_naf = replace_na(group_naf, "AZ")) |> 
  select(-idcom.ind)

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

# surf_by_naf_h <- surf_by_naf |> 
#   group_by(idINS) |> 
#   summarize(IDCOM = first(IDCOM),
#             ts = sum(ts),
#             ind = sum(ind))

surf_by_naf_DCLT <- surf_by_naf |> 
  rename(DCLT = idcom, NA5 = group_naf) |> 
  group_by(DCLT, NA5) |> 
  summarize(tsfull = sum(tsfull),
            ind = sum(ind),
            .groups = "drop") |> 
  filter(tsfull>0)

# ---- Estimation des emplois en fonction de la surface ----
# ATTENTION : il y a des emplois dans des communes sans surfaces correspondantes.
# pour les communes dans le périmètre, on utilise le nombre d'individus plutôt que les surfaces
# (pas de surface pour le code NAF)
# Il reste alors les communes hors périmètre (il y a un flux du scot vers ces communes)
# mais elles sont au delà de 33km
# ca fait un flux de 1% qui va être la fuite

(verif <- anti_join(as_tibble(emplois_by_DCLT), surf_by_naf_DCLT, by = c("DCLT", "NA5")))
complement <- ind_ze |> 
  rename(DCLT = idcom) |> 
  inner_join(verif, by=c("DCLT"), relationship = "many-to-many") |> 
  mutate(tsfull = ind)

surf_by_naf_c <- surf_by_naf |>
  filter(tsfull>0) |> 
  bind_rows(complement |> select(idINS, idcom = DCLT, group_naf = NA5, tsfull, ind))

surf_by_naf_c_DCLT <- surf_by_naf_c |> 
  rename(DCLT = idcom, NA5 = group_naf) |> 
  group_by(DCLT, NA5) |> 
  summarize(tsfull = sum(tsfull),
            .groups = "drop")
(verif2 <- anti_join(as_tibble(emplois_by_DCLT), surf_by_naf_c_DCLT, by = c("DCLT", "NA5")))

sum(verif2$emp) / emplois_by_DCLT[, sum(emp)]
sum(verif2$emp_scot) / emplois_by_DCLT[, sum(emp_scot)]
library(MetricsWeighted)

emplois_by_DCLT_c <- emplois_by_DCLT |> 
  left_join(surf_by_naf_c_DCLT, by = c("DCLT", "NA5")) |> 
  ungroup()

surf2emp <- lm(log(emp)~log(tsfull)*NA5, data = emplois_by_DCLT_c)
gsurf2emp <- ggplot(emplois_by_DCLT_c)+
  aes(x=tsfull, y=emp, col=NA5) +
  geom_point(alpha=.5)+
  facet_wrap(vars(NA5))+
  scale_x_log10("surface professionnelle", labels=ofce::f2si2)+ scale_y_log10("emploi (MOBPRO)") + 
  geom_abline(slope=1, col="orange")+
  geom_smooth(method = "lm", show.legend = FALSE) +
  ofce::theme_ofce()
source("secrets/azure.R")
bd_write(gsurf2emp)
bd_write(emplois_by_DCLT_c)
# emplois_by_DCLT_c |> 
#   mutate(se = emp/ts) |> 
#   group_by(NA5) |> 
#   reframe(q = weighted_quantile(se, w=emp, probs= c(0.1, .5, .9)), qq = c(0.1, .5, .9))

emp_pred.tib <- surf_by_naf_c |>
  rename(DCLT = idcom, 
         NA5 = group_naf) |> 
  select(-ind) |> 
  filter(tsfull > 0) |> 
  inner_join(emplois_by_DCLT_c, by=c("DCLT", "NA5"), suffix = c("", ".com")) 
emp_pred <- exp(predict(surf2emp , newdata = emp_pred.tib |> select(NA5, tsfull)))
emp_pred.tib <- emp_pred.tib |> 
  mutate(
    emp_pred = !!emp_pred,
    emp_pred_scot = emp_pred / emp * emp_scot) |> 
  group_by(DCLT, NA5) |> 
  mutate( 
    emp_pred  = emp_pred / sum(emp_pred) * emp,
    emp_pred_scot = emp_pred / emp * emp_scot) |> 
  select(emp_pred, emp_pred_scot, idINS, NA5, DCLT) |> 
  group_by(idINS) |> 
  summarize(
    DCLT = first(DCLT),
    emp_pred = sum(emp_pred, na.rm=TRUE),
    emp_pred_scot = sum(emp_pred_scot, na.rm=TRUE))

qs::qsave(emp_pred.tib, emp_pred_file)

gc()
# on génére ensuite les paires d'idINS pertinentes (ie associées à un flux mobpro)
# n'est plus utile
# com_paires <- mobilites95 |> 
#   rename(mode = TRANS) |> 
#   group_by(COMMUNE, DCLT, mode) |> 
#   summarize(emp_scot = sum(NB_in, na.rm=TRUE),
#             .groups = "drop") |> 
#   pivot_wider(id_cols = c(COMMUNE, DCLT),
#               names_from = mode, 
#               values_from = c(emp_scot), values_fill = 0) |> 
#   transmute(COMMUNE, DCLT, across(c(car, walk, bike, transit), ~ .x>0)) |> 
#   filter(car|walk|bike|transit)
# 
# c200ze <- c200ze |> 
#   st_drop_geometry() 
# 
# paires_mobpro <- pmap_dfr(com_paires, ~{
#   from <- c200ze |> 
#     filter(com22==..1, ind>0) |>
#     select(fromidINS = idINS)
#   if(nrow(from)>0) {
#     from_4326 <- r3035::idINS2lonlat(from$fromidINS) |> 
#       rename(o_lon = lon, o_lat = lat)
#     from <- from |> 
#       bind_cols(from_4326)
#   }
#   to <- emp_pred |>
#     filter(DCLT==..2, emp_pred>0) |> 
#     select(toidINS = idINS)
#   if(nrow(to)>0) {
#     to_4326 <- r3035::idINS2lonlat(to$toidINS) |> 
#       rename(d_lon = lon, d_lat = lat)
#     to <- to |> 
#       bind_cols(to_4326)
#   }
#   if(length(to)>0&length(from)>0) {
#     it <- dplyr::cross_join(from, to)
#     it <- it |> 
#       dplyr::mutate(COMMUNE = .x, DCLT = .y, car = ..3, walk = ..4, bike = ..5, transit = ..6,
#              euc = r3035::idINS2dist(fromidINS, toidINS)/1000) 
#   } else
#     it <- tibble::tibble()
#   it
# }) 
# unlink(paires_mobpro_dataset)
# arrow::write_dataset(paires_mobpro, paires_mobpro_dataset, partitioning = "COMMUNE")
# 
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
