setwd("~/marseille")
# init ---------------
library(tidyverse)
library(tidytransit)
library(terra)
library(tictoc)
# remotes::install_github("OFCE/accesstars")
library(accesstars) 
library(tmap)
# pak::pak("xtimbeau/accessibility")
library(accessibility)
library(r3035)
library(glue)
library(stars)
library(data.table)
library(conflicted)
library(sitools)
library(arrow)
library(furrr)
library(ofce)
progressr::handlers(global = TRUE)
progressr::handlers("cli")
data.table::setDTthreads(8)
arrow::set_cpu_count(8)

conflict_prefer_all( "dplyr", quiet=TRUE)
conflict_prefer('wday', 'lubridate', quiet=TRUE)

## globals --------------------
load("baselayer.rda")

# OSM en format silicate ------------
# c'est enregistré, donc on peut passer si c'est  déjà fait
osm_file <- glue("{mdir}/osm.qs")
if(!file.exists(osm_file)) {
  zone <- qs::qread(communes_ar_file) |> 
    filter(mobpro99) |> 
    summarize()
  osm <- download_osmsc(zone, elevation = TRUE, workers = 16)
  qs::qsave(osm, osm_file)
  rm(osm)
  gc()
}

dodgr_router <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                    osm = osm_file,
                                    mode = "CAR", 
                                    turn_penalty = TRUE,
                                    distances = TRUE,
                                    denivele = TRUE,
                                    n_threads = 16L,
                                    overwrite = FALSE,
                                    nofuture = TRUE)

# premier essai, infructueux pour le moment
# plan("multisession", workers = 4L)
# paires <- open_dataset(paires_mobpro_dataset)
# 
# unlink("{mdir}/temp_dodgr" |> glue(), recursive = TRUE)
# 
# communes <- paires |> distinct(COMMUNE) |> collect() |> pull()
# 
# paire <- paires |> 
#   filter(COMMUNE == communes[[1]]) |> 
#   collect()
# dodgr_pairs(od = paire |> slice(1:5000), routing = dodgr_router)
# 
# deuxième essai, (il y a un pb avec les index)

plan("multisession", workers = 4)

mobpro <- qs::qread(mobpro_file) |> 
  filter(mobpro95) |> 
  group_by(COMMUNE, DCLT, TRANS) |> 
  summarise(emp = sum(NB_in, na.rm=TRUE), .groups = "drop") |> 
  pivot_wider(names_from = TRANS, values_from = emp, values_fill = 0)

COMMUNEs <- mobpro |> distinct(COMMUNE) |> pull()
DCLTs <- mobpro |> distinct(DCLT) |> pull()

idINSes <- qs::qread(c200ze_file) |> 
  st_drop_geometry() |> 
  select(com=com22, idINS, scot, emp_resident, ind) |> 
  mutate(from = scot & (ind>0) & com%in%COMMUNEs,
         to = emp_resident>0 & com %in%DCLTs) |> 
  filter(from | to)

dgr_distances_by_com(idINSes, mobpro, dodgr_router)  

dgr_distances_by_paires(idINSes, mobpro, dodgr_router, chunk = 10000)  

dgr_distances_full(idINSes, dodgr_router, chunk = 5000000)  

dgr_distances_by_com(idINSes, mobpro |> filter(walk>0), dodgr_router, path = "dgr_walk")  

# tentative d'optimisation

com2com <- mobpro |>
  filter(car>0) |> 
  select(COMMUNE, DCLT) |> 
  left_join(idINSes |> filter(from) |> count(com, name = "nfrom"), 
            by=c("COMMUNE"="com")) |> 
  left_join(idINSes |> filter(to) |> count(com, name = "nto"),
            by=c("DCLT"="com")) |> 
  mutate(n = nfrom*nto)

nDCLT <- com2com |> distinct(DCLT, nto) |> pull(nto, name = DCLT)
nCOMMUNE <- com2com |> distinct(COMMUNE, nfrom) |> pull(nfrom, name = COMMUNE)

dclts <- com2com |> 
  group_by(COMMUNE) |>
  summarize(dclt = list(DCLT), grids = list(nto), nfrom = first(nfrom)) |>
  mutate(l = map_int(dclt, length)) |>
  mutate( enplus = map(dclt, ~setdiff(.x,unlist(dclt[COMMUNE==13001]))),
          enmoins = map(dclt, ~setdiff(unlist(dclt[COMMUNE==13001]), .x)),
          union = map(dclt, ~unique(c(.x, unlist(dclt[COMMUNE==13001]))))) |> 
  mutate(n_union = map_int(union, ~sum(nDCLT[.x]) ) * (nfrom+(1-(COMMUNE==13001))*nfrom[COMMUNE==13001]),
         n_seul = map_int(dclt, ~sum(nDCLT[.x]) ) * nfrom,
         perte = 1-(n_seul+(1-(COMMUNE==13001))*n_seul[COMMUNE==13001])/n_union) |> 
  arrange(desc(perte))

dclts <- com2com |> group_by(COMMUNE) |> summarize(dclt = list(DCLT), grids = list(nto), nfrom = first(nfrom)) |> mutate(l = map_int(dclt, length)) |> mutate( enplus = map(dclt, ~setdiff(.x,unlist(dclt[COMMUNE==13001]))))

# distance pairique -----------
# la distance est définie comme d = (np_union - np_séparés)/(nfrom1 * nto2 + nfrom2 * nto1)

d_pairique <- function( com1, com2, data = com2com) {
  if(com1==com2)
    return(0)
  com1 <- as.character(com1)
  com2 <- as.character(com2)
  nDCLT <- data |> distinct(DCLT, nto) |> pull(nto, name = DCLT)
  nCOMMUNE <- data |> distinct(COMMUNE, nfrom) |> pull(nfrom, name = COMMUNE)
  n1 <- nCOMMUNE[[com1]]
  n2 <- nCOMMUNE[[com2]]
  nto1 <- sum((data |> filter(COMMUNE==com1) |> pull(nto)))
  nto2 <- sum((data |> filter(COMMUNE==com2) |> pull(nto)))
  np_sep <- n1 * nto1 + n2 * nto2
  np_union <- (n1+n2) * sum(nDCLT[(data |> filter(COMMUNE%in% c(com1, com2)) |> distinct(DCLT) |> pull())])
  np_cross <- n1 * nto2 + n2 * nto1
  return((np_union-np_sep)/ np_cross)
}
d_pairique(13001, 13002)
plan("multisession", workers = 16)
tt <- cross_join(com2com |> distinct(COMMUNE), com2com |> distinct(COMMUNE)) |> 
  mutate(d = future_map2_dbl(COMMUNE.x, COMMUNE.y, d_pairique, .progress=TRUE))
mm <- tt |> arrange(COMMUNE.x, COMMUNE.y) |> 
  pivot_wider( names_from = COMMUNE.y, id_cols = COMMUNE.x, values_from = d) |> 
  select(-COMMUNE.x) |> 
  as.matrix()
rownames(mm) <- colnames(mm)
clust <- factoextra::hcut(as.dist(mm), method = "complete", k=4)
ggdendro::ggdendrogram(clust, rotate = TRUE)
best.cut <- NbClust::NbClust(diss=as.dist(mm), method="ward.D", distance=NULL, index="silhouette")
best.cut$Best.partition |> sort()

kk <- map_dfr(1:20, ~com2com |> 
                mutate(clust  = factoextra::hcut(as.dist(mm), method = "complete", k=.x)$cluster[COMMUNE]) |> 
                group_by(clust) |> 
                summarize(ncom = sum(nCOMMUNE[unique(COMMUNE)]) ,
                          ndclt = sum(nDCLT[unique(DCLT)]),
                          n = ncom*ndclt) |> 
                summarize( 
                  k = .x,
                  total_k = sum(n)/1000000,
                  min_k = min(ncom*ndclt)/1000000,
                  total = sum(nCOMMUNE)*sum(nDCLT)/1000000, 
                  ecart = sum(n)/(sum(nCOMMUNE)*sum(nDCLT))))


