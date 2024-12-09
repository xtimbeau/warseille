library(r3035)
library(ofce)
library(tidyverse)
library(archive)
library(sf)
library(mapboxapi)
library(stars)
library(glue)
library(conflicted)
library(fs)
library(readxl)
library(rlang)
library(glue)
library(stringr)
library(sf)
library(writexl)
library(dplyr)
conflict_prefer_all("dplyr", quiet = TRUE)
source("mglobals.r")
# bloque les downloads
download <- TRUE

# IRIS ------------------

if(download|!file.exists(iris_file)) {
  curl::curl_download(
    "https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/CONTOURS-IRIS_2-1__SHP__FRA_2022-01-01/CONTOURS-IRIS_2-1__SHP__FRA_2022-01-01.7z",
    destfile = "/tmp/iris2022.7z" |> glue())
  zz <- archive_extract("/tmp/iris2022.7z", dir = "/tmp" |> glue())
  shp <- zz |> keep(~str_detect(.x, "LAMB93")) |> keep(~str_detect(.x, ".shp"))
  iris22sf <- st_read(str_c("/tmp/",shp))
  curl::curl_download(
    url = "https://www.insee.fr/fr/statistiques/fichier/7708995/reference_IRIS_geo2022.zip",
    destfile = "/tmp/tableiris2022.zip" |> glue())
  zz <- archive_extract("https://www.insee.fr/fr/statistiques/fichier/7708995/reference_IRIS_geo2022.zip", dir = "/tmp")
  refiris22 <- readxl::read_xlsx("/tmp/reference_IRIS_geo2022.xlsx", skip=5)
  
  iris22 <- left_join(
    refiris22, as_tibble(iris22sf) |> select(CODE_IRIS, geometry), by = "CODE_IRIS") |> 
    st_as_sf() |> st_transform(3035)
  
  qs::qsave(iris22, iris_file)
}

# zz <- archive_extract("https://www.insee.fr/fr/statistiques/fichier/7233950/BASE_TD_FILO_DISP_IRIS_2020_CSV.zip", dir ="/tmp")
# revIRIS <- vroom::vroom(str_c("/tmp/", zz[1]))
# 
# iris22 <- iris22 |> left_join(revIRIS |> select(CODE_IRIS = IRIS, DISP_MED20), by = "CODE_IRIS")

# carroyage c200 --------------------
if(!file.exists(c200_file)|download) {
  curl::curl_download(
    "https://www.insee.fr/fr/statistiques/fichier/6215138/Filosofi2017_carreaux_200m_shp.zip",
    destfile="/tmp/carreaux.zip") 
  unzip("/tmp/carreaux.zip", exdir = "/tmp")
  archive_extract("/tmp/Filosofi2017_carreaux_200m_shp.7z", dir="/tmp")
  
  c200 <- st_read("/tmp/Filosofi2017_carreaux_200m_met.shp" |> glue(), stringsAsFactors=FALSE) |> 
    mutate(idINS = str_replace(Idcar_200m, "CRS3035RES200m", "r200")) |> 
    st_transform(3035)
  
  iris22 <- qs::qread(iris_file) |>
    filter(!st_is_empty(geometry)) |> 
    st_make_valid() |> 
    mutate(id = 1:n())
  
  irises <- sf::st_intersects(c200, iris22) 
  names(irises) <- 1:length(irises)
  l_irises  <- map_int(irises, length)
  nas <- sf::st_nearest_feature(c200[l_irises==0,"Idcar_200m"], iris22)
  irises[l_irises==0] <- nas
  
  l2 <- st_join(
    c200[names(irises[l_irises>=2]),"Idcar_200m" ], 
    iris22[, "id"], largest=TRUE)
  
  irises[l_irises>=2] <- l2 |> pull(id)
  flat_irises <- purrr::list_c(irises)
  
  # on prend les iris pour être cohérent, un carreau peut être sur plusieurs communes
  # on utilise donc la géographie de iris18
  
  c200 <- c200 |>
    transmute(
      idINS = contract_idINS(idINS),
      dep=str_sub(lcog_geo, 1,2),
      com22=iris22$DEPCOM[flat_irises], 
      CODE_IRIS = iris22$CODE_IRIS[flat_irises],
      ind = Ind,
      men = Men,
      enfants = Ind_0_3+Ind_4_5+Ind_6_10+Ind_11_17,
      adultes = Ind_18_24+Ind_25_39+Ind_40_54+Ind_55_64+Ind_65_79+Ind_80p,
      ind_18_64 = Ind_18_24+Ind_25_39+Ind_40_54+Ind_55_64,
      ind_snv = Ind_snv
    )
  
  qs::qsave(c200, c200_file)
}
# GEOGRAPHIES DES COMMUNES --------------------

if(!file.exists(com2021_file)|!file.exists(com2017_file)|download) {
  curl::curl_download(
    "http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20170111-shp.zip",
    destfile = "/tmp/communes_2017.zip")
  unzip("/tmp/communes_2017.zip", exdir = "/tmp")
  com2017 <- read_sf("/tmp/communes-20170112.shp")
  qs::qsave(com2017, com2017_file)
  curl::curl_download(
    "http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20210101-shp.zip",
    destfile = "/tmp/communes_2021.zip")
  unzip("/tmp/communes_2021.zip", exdir = "/tmp")
  com2021 <- read_sf("/tmp/communes-20210101.shp")
  qs::qsave(com2021, com2021_file)
}

com2021 <- qs::qread(com2021_file)

# DEFINITION DES ZONES ---------------------------------------------------------
# on récupère les epci de 2017 pour ne pas perdre Péré
archive_extract(
  "https://www.insee.fr/fr/statistiques/fichier/2510634/Intercommunalite_Metropole_au_01-01-2017.zip",
  dir = "/tmp" |> glue())

curl::curl_download(
  "https://www.insee.fr/fr/statistiques/fichier/2510634/Intercommunalite_Metropole_au_01-01-2017.zip", 
  destfile = "/tmp/Intercommunalite_Metropole_au_01-01-2017.zip")
unzip("/tmp/Intercommunalite_Metropole_au_01-01-2017.zip", exdir = "/tmp")

epcis <- readxl::read_xls(
  "/tmp/Intercommunalite_Metropole_au_01-01-2017.xls" |> glue(),
  sheet=2,
  skip=5)

scot_tot.epci <- epcis |>
  dplyr::filter(str_detect(LIBGEO, str_c(scot_tot.n, collapse='|'))) |>
  pull(CODGEO, name=LIBGEO)

# commune plus arrondissements ----------------

if(!file.exists(communes_ar_file)|download) {
  dir.create("/tmp/communes")
  curl::curl_download(
    "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS/ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2023-09-21/ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2023-09-21.7z",
    destfile = "/tmp/communes/adx.7z")
  
  commune <- archive_extract("/tmp/communes/adx.7z", 
                             files = archive("/tmp/communes/adx.7z") |> filter(str_detect(path, "/COMMUNE\\.")) |> pull(path),
                             dir = "/tmp/communes")
  
  ar <- archive_extract("/tmp/communes/adx.7z", 
                        files = archive("/tmp/communes/adx.7z") |> filter(str_detect(path, "/ARRONDISSEMENT_MUNICIPAL\\.")) |> pull(path),
                        dir = "/tmp/communes")
  
  communes <- sf::st_read(str_c("/tmp/communes/", commune |> purrr::keep(~stringr::str_detect(.x, ".shp"))))
  
  ars <- sf::st_read(str_c("/tmp/communes/", ar |> purrr::keep(~stringr::str_detect(.x, ".shp")))) 
  com_ars <- unique(ars$INSEE_COM) 
  ars <- tibble(ars) |> 
    mutate(COMMUNE = INSEE_COM, INSEE_COM = INSEE_ARM, ar = TRUE, com = FALSE) |>
    select(-INSEE_ARM) |> 
    left_join(
      communes |> 
        st_drop_geometry() |> 
        select(INSEE_COM, INSEE_CAN, INSEE_ARR,
               INSEE_DEP, INSEE_REG, SIREN_EPCI), 
      by=c("COMMUNE"="INSEE_COM"))
  
  communes <- as_tibble(communes) |>
    mutate(COMMUNE = INSEE_COM, ar = !(COMMUNE %in% com_ars), com = TRUE) |> 
    bind_rows(ars) |> 
    st_as_sf() |> 
    st_transform(3035)
  
  qs::qsave(communes, communes_ref_file)
}

communes <- qs::qread(communes_ref_file) |> 
  filter(ar)

com2021epci <- communes |> 
  filter(SIREN_EPCI %in% epci.metropole) |> 
  mutate(EPCI = SIREN_EPCI) |> 
  st_transform(3035)

geoepci <- com2021epci |> 
  group_by(EPCI) |> 
  summarize() |>
  st_transform(3035)

# là c'est où c'est possible de faire des modifications aux iris 

zone_emploi <- geoepci |>
  st_union() |>
  st_buffer(66000) |> 
  st_as_sf()

petite_zone_emploi <- geoepci |>
  st_union() |>
  st_buffer(33000) |> 
  st_as_sf()

# pour être sûr d'avoir la commune en entier, on arrondit à la commune
communes_ze <- communes |> 
  st_filter(zone_emploi) |> 
  mutate(pze = map_lgl(st_intersects(geometry, petite_zone_emploi), ~length(.x)==1))

qs::qsave(communes_ze, communes_file)

communes_pze <- communes_ze |> filter(pze) |> pull(INSEE_COM)
communes_ze <- communes_ze |> pull(INSEE_COM)

communes <- communes |> 
  mutate(
    pze = INSEE_COM %in% communes_pze,
    ze = INSEE_COM %in% communes_ze,
    scot = SIREN_EPCI %in% epci.metropole)

qs::qsave(communes, communes_ar_file)
bd_write(communes)

iris <- qs::qread(iris_file)
irises <- st_join(zone_emploi, iris) |> pull(CODE_IRIS)
# c200i <- qs::qread(c200_file) |> 
#   filter(CODE_IRIS%in%irises)

# # GTFS --------------------
# if(download) {
#   fs::dir_create(localr5)
#   fs::dir_create(localr5_alt)
#   
#   curl::curl_download(
#     "https://www.arcgis.com/sharing/rest/content/items/1e0aad2349db4571b3c91087e58367e8/data",
#     destfile = "{localr5}/GTFS_SMTC_Clermont.zip" |> glue())
#   
#   fs::file_copy("{localr5}/GTFS_SMTC_Clermont.zip" |> glue(),
#                 "{localr5_alt}/GTFS_SMTC_Clermont.zip" |> glue(),
#                 overwrite = TRUE)
#}

# base_emploi <- read_xlsx("~/files/base-cc-emploi-pop-active-2018.xlsx")

# fond de carte -------------------

mblr3 <- mapboxapi::get_static_tiles(
  location = st_union(com2021epci) |> st_buffer(-1000) |> st_transform(4326),
  zoom=10, 
  style_id = "ckjka0noe1eg819qrhuu1vigs", 
  username="xtimbeau",
  access_token = Sys.getenv("mapbox_token")) 
mblr3clair <- 255 - (255-mblr3)*0.75
# mblr3 <-(st_as_stars(mblr3)[,,,1:3])|> st_transform(3035) |> st_rgb()
mblr3 <- st_as_stars(mblr3)[,,,1:3]
mblr3clair <- st_as_stars(mblr3clair)[,,,1:3]
bbx <- st_bbox(com2021epci)

mblr3_large <- mapboxapi::get_static_tiles(
  location = st_union(communes |> filter(ze)) |> st_buffer(-1000) |> st_transform(4326),
  zoom=10, 
  style_id = "ckjka0noe1eg819qrhuu1vigs", 
  username="xtimbeau",
  access_token = Sys.getenv("mapbox_token")) 
mblr3clair_large <- 255 - (255-mblr3_large)*0.75
mblr3_large <- st_as_stars(mblr3_large)[,,,1:3]
mblr3clair_large <- st_as_stars(mblr3clair_large)[,,,1:3]

decor_carte <- list(
  ggspatial::layer_spatial(mblr3clair) ,
  geom_sf(data=com2021epci, col="gray50", fill=NA, alpha=1, linewidth=0.1),
  geom_sf(data=geoepci, col="gray25", fill=NA, alpha=1, linetype = "dotted", linewidth=0.25),
  coord_sf(),
  xlab(NULL),ylab(NULL),labs(title=NULL),
  scale_x_continuous(expand=c(0,0)),
  scale_y_continuous(expand=c(0,0)),
  annotate(
    "label", x=Inf, y=-Inf, label = "\U00A9 Mapbox, \U00A9 OpenStreetMap",
    hjust=1, vjust=0, size=2, label.padding = unit(4, "pt"),
    label.size = 0, fill="gray98", alpha=0.5),
  theme_ofce_void(),
  ggspatial::annotation_scale(
    line_width = 0.2, height = unit(0.1, "cm"), 
    text_cex = 0.4, pad_y = unit(0.1, "cm")),
  theme(
    text = element_text(family = "Roboto"),
    legend.key.size = unit(0.5, "cm"),
    legend.title = element_text(size=6, margin=margin(1,1,1,1, "pt")),
    legend.text =  element_text(size=5), legend.justification = c(1,0.9)))

bd_write(decor_carte)
save(decor_carte, file=decor_carte_file)

decor_carte_large <- list(
  ggspatial::layer_spatial(mblr3clair_large) ,
  geom_sf(data=communes |> filter(ze), col="gray75", fill=NA, alpha=1, linewidth=0.1),
  geom_sf(data=geoepci, col="gray25", fill=NA, alpha=1, linetype = "dotted", linewidth=0.25),
  coord_sf(),
  xlab(NULL),ylab(NULL),labs(title=NULL),
  scale_x_continuous(expand=c(0,0)),
  scale_y_continuous(expand=c(0,0)),
  annotate(
    "label", x=Inf, y=-Inf, label = "\U00A9 Mapbox, \U00A9 OpenStreetMap",
    hjust=1, vjust=0, size=2, label.padding = unit(4, "pt"),
    label.size = 0, fill="gray98", alpha=0.5),
  theme_ofce_void(),
  ggspatial::annotation_scale(
    line_width = 0.2, height = unit(0.1, "cm"), 
    text_cex = 0.4, pad_y = unit(0.1, "cm")),
  theme(
    text = element_text(family = "Roboto"),
    legend.key.size = unit(0.5, "cm"),
    legend.title = element_text(size=6, margin=margin(1,1,1,1, "pt")),
    legend.text =  element_text(size=5), legend.justification = c(1,0.9)))

bd_write(decor_carte_large)

# save -------------
save(list = c(bl, "geoepci", "com2021epci",
              "zone_emploi", "petite_zone_emploi",
              "communes_ze", "communes_pze"), file="baselayer.rda")
