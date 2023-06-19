library(tidyverse)
library(archive)
# devtools::install_github("ofce/r3035")
library(r3035)
library(sf)
library(mapboxapi)
library(stars)
library(glue)
library(conflicted)
# devtools::install_github("ofce/ofce")
library(ofce)
library(fs)
library(readxl)
conflict_prefer_all("dplyr", quiet = TRUE)

bl <- load("~/marseille/baselayer.rda")

# bloque les downloads
download <- TRUE

# IRIS ------------------

rep <- str_c(temp_dir, "/iris2018")
if(download|!file.exists("{DVFdata}/iris18.qs" |> glue())) {
  if(fs::dir_exists(rep)) fs::dir_delete(rep)
  fs::dir_create(rep)
  # 2018
  curl::curl_download(
    "https://wxs.ign.fr/1yhlj2ehpqf3q6dt6a2y7b64/telechargement/inspire/CONTOURS-IRIS-2018-01-02$CONTOURS-IRIS_2-1__SHP__FRA_2018-01-01/file/CONTOURS-IRIS_2-1__SHP__FRA_2018-01-01.7z", 
    glue("{rep}/iris18.7z"))
  archive_extract(glue("{rep}/iris18.7z"), dir=rep)
  
  files <- fs::dir_ls(rep, recurse = TRUE)
  iris_shp <- files[str_detect(files, "CONTOURS-IRIS.shp")&str_detect(files, "LAMB93")]
  
  iris18.cont <- st_read(iris_shp)
  
  archive_extract("https://www.insee.fr/fr/statistiques/fichier/2017499/reference_IRIS_geo2018.zip", 
                  dir = rep)
  
  iris18.table <- read_excel("{rep}/reference_IRIS_geo2018.xls" |> glue(), skip = 5)
  
  iris18 <- left_join(iris18.table, 
                      as_tibble(iris18.cont) |> select(CODE_IRIS, geometry), 
                      by = "CODE_IRIS") |> 
    st_as_sf() |> 
    st_transform(3035) 
  
  qs::qsave(iris18, file="{DVFdata}/iris18.qs" |> glue())
  fs::dir_delete(rep)
}
# C200 ---------------
if(download|!file.exists(c200_file)) {
  
  caro <- "~/files/DVFdata/sources/carreau"
  
  if(fs::dir_exists(caro)) fs::dir_delete(caro)
  fs::dir_create(caro)
  
  archive_extract("https://www.insee.fr/fr/statistiques/fichier/6215138/Filosofi2017_carreaux_200m_shp.zip",
                  dir = "{caro}/2017/" |> glue())
  archive_extract("{caro}/2017/Filosofi2017_carreaux_200m_shp.7z" |> glue(),
                  dir = "{caro}/2017/" |> glue())
  c200 <- st_read("{caro}/2017/Filosofi2017_carreaux_200m_met.shp" |> glue(), stringsAsFactors=FALSE)
  
  fs::dir_delete(caro)
  
  c200 <- c200 |> st_transform(3035) 
  xy <- c200 |> st_centroid()
  idINS200 <- idINS3035(xy |> st_coordinates(), resolution=200)
  
  irises <- sf::st_intersects(c200, iris18) 
  irises <- map_int(irises , ~ifelse(length(.x)==0, NA, .x[[1]]))
  nas <- sf::st_nearest_feature(c200[is.na(irises),], iris18)
  irises[is.na(irises)] <- nas
  
  # on prend les iris pour être cohérent, un carreau peut être sur plusieurs communes
  # on utilise donc la géographie de iris18
  
  c200f <- c200 |>
    transmute(
      idINS=idINS200, 
      dep=str_sub(lcog_geo, 1,2),
      com=iris18$DEPCOM[irises], 
      CODE_IRIS = iris18$CODE_IRIS[irises],
      ind = Ind,
      men = Men,
      enfants = Ind_0_3+Ind_4_5+Ind_6_10+Ind_11_17,
      ecoliers_mat = Ind_4_5,
      ecoliers_prim = Ind_6_10,
      collegienslyceens = Ind_11_17,
      adultes = Ind_18_24+Ind_25_39+Ind_40_54+Ind_55_64+Ind_65_79+Ind_80p,
      ind_18_64 = Ind_18_24+Ind_25_39+Ind_40_54+Ind_55_64,
      ind_snv = Ind_snv
    )
  
  qs::qsave(c200f, c200_file)
}
# GEOGRAPHIES DES COMMUNES --------------------

com2017_rep <- "{DVFdata}/sources/Communes/2017" |> glue()
no_file <- !exists("com2017_shp")||!file.exists(com2017_shp)

if(download|no_file) {
  unlink(com2017_rep, recursive = TRUE, force = TRUE)
  dir.create(com2017_rep)
  curl::curl_download(
    'http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20170111-shp.zip',
    destfile = "{com2017_rep}/downloaded.zip" |> glue())
  unzip("{com2017_rep}/downloaded.zip" |> glue(),
        exdir = com2017_rep)
}
com2017_shp <- fs::dir_ls(com2017_rep, glob="*.shp")[[1]]

com2021_rep <- "{DVFdata}/sources/Communes/2021" |> glue()
no_file <- !exists("com2021_shp")||!file.exists(com2021_shp)
if(download|no_file) {
  if(fs::dir_exists(com2021_rep)) fs::dir_delete(com2021_rep)
  dir.create(com2021_rep)
  curl::curl_download(
    'http://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20210101-shp.zip',
    destfile = "{com2021_rep}/2021downloaded.zip" |> glue())
  unzip("{com2021_rep}/2021downloaded.zip" |> glue(),
        exdir = com2021_rep)
}
com2021_shp <- fs::dir_ls(com2021_rep, glob="*.shp")[[1]]

# DEFINITION DES ZONES ---------------------------------------------------------
# on récupère les epci de 2017 pour ne pas perdre Péré
if(download|!file.exists("{localdata}/Intercommunalite_Metropole_au_01-01-2017.xls" |> glue())) {
  archive_extract(
    "https://www.insee.fr/fr/statistiques/fichier/2510634/Intercommunalite_Metropole_au_01-01-2017.zip", 
    dir = "{localdata}/" |> glue())
}

epcis <- readxl::read_xls(
  "{localdata}/Intercommunalite_Metropole_au_01-01-2017.xls" |> glue(),
  sheet=2,
  skip=5)

scot1.epci <- epcis |> 
  dplyr::filter(str_detect(LIBEPCI, str_c(scot1.n, collapse='|'))) |>
  pull(CODGEO, name=LIBGEO)

scot2.epci <- epcis |>
  dplyr::filter(str_detect(LIBEPCI, str_c(scot2.n, collapse='|'))) |> 
  pull(CODGEO, name=LIBGEO)


scot3.epci <- epcis |> 
  dplyr::filter(str_detect(LIBEPCI, str_c(scot3.n, collapse='|'))) |>
  pull(CODGEO, name=LIBGEO)

scot4.epci <- epcis |>
  dplyr::filter(str_detect(LIBEPCI, str_c(scot4.n, collapse='|'))) |> 
  pull(CODGEO, name=LIBGEO)

scot5.epci <- epcis |> 
  dplyr::filter(str_detect(LIBEPCI, str_c(scot5.n, collapse='|'))) |>
  pull(CODGEO, name=LIBGEO)

scot_tot.epci <- epcis |>
  dplyr::filter(str_detect(LIBEPCI, str_c(scot_tot.n, collapse='|'))) |> 
  pull(CODGEO, name=LIBGEO)


geoepci <- map_dfr(scot_tot.n, ~{
  coms <- epcis |> 
    dplyr::filter(str_detect(LIBEPCI, .x)) |>
    pull(CODGEO, name=LIBGEO)
  st_read(com2017_shp) |> 
    dplyr::filter(insee%in%coms) |>
    summarise() |>
    mutate(epci = .x)
}) |> st_transform(3035)

# version MAJ d'iris (en espérant que toutes les communes y sont)
iris <- qs::qread("{DVFdata}/iris18.qs" |> glue())
# si il y a besoin de mettre à jour (même si le nouveau fichier IRIS est mis à jour en 2022)

iris <- iris  |> 
  rename(COM = DEPCOM, IRIS = CODE_IRIS) 

# là c'est où c'est possible de faire des modifications aux iris 

scot_tot <- iris |>
  dplyr::filter(COM %in% scot_tot.epci) |>
  st_union()
# scot4 <- iris |>
#   dplyr::filter(COM %in% scot4.epci) |>
#   st_union()
zone_emploi <- scot_tot |>
  st_union() |>
  st_buffer(33000)
communes.scot_tot <- iris |>
  dplyr::filter(COM %in% scot_tot.epci) |>
  group_by(COM) |>
  summarize()
c200 <- qs::qread(c200_file)
c200.scot3 <- c200 |> filter(st_intersects(c200, scot3, sparse=FALSE))
c200.scot4 <- c200 |> filter(st_intersects(c200, scot4, sparse=FALSE))

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

# fond de carte -------------------

mblr3 <- mapboxapi::get_static_tiles(
  location = scot_tot |> st_buffer(-1000) |> st_transform(4326),
  zoom=10, 
  style_id = "ckjka0noe1eg819qrhuu1vigs", 
  username="xtimbeau") 
mblr3clair <- 255 - (255-mblr3)*0.75
# mblr3 <-(st_as_stars(mblr3)[,,,1:3])|> st_transform(3035) |> st_rgb()
mblr3 <- st_as_stars(mblr3)[,,,1:3]
mblr3clair <- st_as_stars(mblr3clair)[,,,1:3]
bbx <- st_bbox(communes.scot_tot)

decor_carte <- list(
  ggspatial::layer_spatial(mblr3clair) ,
  geom_sf(data=communes.scot_tot, col="gray70", fill="gray96", alpha=0.1, size=0.1),
  geom_sf(data=geoepci, col="gray60", fill="gray96", alpha=0.1, size=0.2),
  coord_sf(),
  xlab(NULL),ylab(NULL),labs(title=NULL),
  scale_x_continuous(expand=c(0,0)),
  scale_y_continuous(expand=c(0,0)),
  annotate(
    "label", x=Inf, y=-Inf, label = "\U00A9 Mapbox, \U00A9 OpenStreetMap",
    hjust=1, vjust=0, size=2, label.padding = unit(4, "pt"),
    label.size = 0, fill="gray98", alpha=0.5),
  theme_void(),
  ggspatial::annotation_scale(
    line_width = 0.2, height = unit(0.1, "cm"), 
    text_cex = 0.4, pad_y = unit(0.1, "cm")),
  theme(
    text = element_text(family = "Roboto"),
    legend.key.size = unit(0.5, "cm"),
    legend.title = element_text(size=6, margin=margin(1,1,1,1, "pt")),
    legend.text =  element_text(size=5), legend.justification = c(1,0.9)))

save(decor_carte, file=decor_carte_file)

# save -------------
save(list = c(bl, "scot1", "scot2", "scot3", "scot4", "scot5", "scot_tot", "scot3.epci", "geoepci",
              "zone_emploi",
              "com2021_shp", "com2017_shp", "communes.scot_tot",
              "mblr3", "mblr3clair"), file="baselayer.rda")