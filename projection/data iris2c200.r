library(archive)
library(tidyverse)
library(glue)
library(data.table)
library(tmap)
library(sf)
library(conflicted)
conflicted::conflict_prefer_all("dplyr", quiet = TRUE)
load("baselayer.rda")
# on prépare les données pour l'extrapolation des modèles
# les données sont sourcées à l'IRIS, on extrapole au carreau
dir.create("projection/data")

# revenus
archive_extract("https://www.insee.fr/fr/statistiques/fichier/5055909/BASE_TD_FILO_DEC_IRIS_2018.zip",
                dir = "projection/data")
rev_iris <- read_delim("projection/data/BASE_TD_FILO_DEC_IRIS_2018.csv") |>
  rename_with(~str_remove(.x, "18$")) |> 
  rename_with(~str_remove(.x, "^DEC_")) |> 
  rename(D5 = MED) |> 
  relocate(IRIS, D1, D2, D3, D4, D5, D6, D7, D8, D9)

d1N <- 11500
d5N <- 21730
d9N <- 39430

d25N <- (.25-.1)/(.5-.1)*(d5N-d1N)+ d1N
d75N <- (.75-.5)/(.9-.5)*(d9N-d5N)+ d5N

s_quartiles <- rev_iris |>
  drop_na(D1:D9) |> 
  rowwise() |> 
  transmute(
    IRIS, 
    sq1 = approx(x=c(D1, D2, D3, D4, D5, D6, D7, D8, D9), y=1:9/10, xout=d25N, rule = 2)$y,
    sq2 = approx(x=c(D1, D2, D3, D4, D5, D6, D7, D8, D9), y=1:9/10, xout=d5N, rule = 2)$y-sq1,
    sq3 = approx(x=c(D1, D2, D3, D4, D5, D6, D7, D8, D9), y=1:9/10, xout=d75N, rule = 2)$y-sq2-sq1,
    sq4 = 1-sq3-sq2-sq1)
## mais les iris ne sont pas connu (loin de la) pour tous les carreaux
## on utilise donc les communes

archive_extract("https://www.insee.fr/fr/statistiques/fichier/5009236/base-cc-filosofi-2018_CSV_geo2021.zip",
                dir = "projection/data")
rev_com <- read_delim("projection/data/cc_filosofi_2018_COM-geo2021.CSV") |> 
  drop_na(MED18, D118, D918) |> 
  rowwise() |> 
  transmute(
    commune = CODGEO,
    sq1 = approx(x=c(D118, MED18, D918), y=c(0.1, 0.5, 0.9), xout=d25N, rule = 2)$y,
    sq2 = approx(x=c(D118, MED18, D918), y=c(0.1, 0.5, 0.9), xout=d5N, rule = 2)$y-sq1,
    sq3 = approx(x=c(D118, MED18, D918), y=c(0.1, 0.5, 0.9), xout=d75N, rule = 2)$y-sq1-sq2,
    sq4 = 1-sq1-sq2-sq3 )
# mais aussi les départements parce que commune ca n'ajoute pas grand chose
# il faudra un jour faire ça sérieusement avec filosofi

archive_extract("https://www.insee.fr/fr/statistiques/fichier/5009236/base-cc-filosofi-2018_CSV_geo2021.zip",
                dir = "projection/data")
rev_dep <- read_delim("projection/data/cc_filosofi_2018_DEP-geo2021.CSV") |> 
  drop_na(MED18, D118, D918) |> 
  rowwise() |> 
  transmute(
    dep = CODGEO,
    sq1 = approx(x=c(D118, MED18, D918), y=c(0.1, 0.5, 0.9), xout=d25N, rule = 2)$y,
    sq2 = approx(x=c(D118, MED18, D918), y=c(0.1, 0.5, 0.9), xout=d5N, rule = 2)$y-sq1,
    sq3 = approx(x=c(D118, MED18, D918), y=c(0.1, 0.5, 0.9), xout=d75N, rule = 2)$y-sq1-sq2,
    sq4 = 1-sq1-sq2-sq3 )

# type de ménages

archive_extract("https://www.insee.fr/fr/statistiques/fichier/5650714/base-ic-couples-familles-menages-2018_csv.zip",
                dir = "projection/data")
men_iris <- read_delim("projection/data/base-ic-couples-familles-menages-2018.CSV") |>
  transmute(
    IRIS,
    tm1 = C18_MENPSEUL,
    tm2 = C18_MENCOUPAENF,
    tm3 = C18_MENCOUPSENF,
    tm4 = C18_MENFAMMONO,
    tm5 = C18_MEN - tm1 - tm2 - tm3 - tm4,
    men_iris = C18_MEN,
    ti1 = C18_PMEN_MENPSEUL,
    ti2 = C18_PMEN_MENCOUPAENF,
    ti3 = C18_PMEN_MENCOUPSENF,
    ti4 = C18_PMEN_MENFAMMONO,
    ti5 = C18_PMEN - ti1 - ti2 - ti3 - ti4,
    ind_iris = C18_PMEN)

# voitures

archive_extract("https://www.insee.fr/fr/statistiques/fichier/5650749/base-ic-logement-2018_csv.zip",
                dir = "projection/data")
voi_iris <- read_delim("projection/data/base-ic-logement-2018.CSV") |> 
  transmute(
    IRIS,
    voiture = P18_RP_VOIT1P/P18_MEN)

# iris4mod

iris4mod.1 <- s_quartiles |> 
  full_join(men_iris, by="IRIS") |> 
  full_join(voi_iris, by="IRIS")

# suite aux misères du CASD on utilise le niveau de vie moyen du carreau 200

c200ze <- qs::qread(c200ze_file)

iris4mod <- c200ze |> 
  filter(scot, ind>0) |> 
  mutate(commune = str_sub(IRIS, 1, 5),
         dep = str_sub(IRIS, 1, 2),
         revuce = ind_snv/ind,
         sq1=NA, sq2=NA, sq3=NA, sq4=NA) |> 
  left_join(s_quartiles, by="IRIS", suffix = c("", ".iris")) |> 
  left_join(rev_com, by="commune", suffix = c("", ".com")) |>
  left_join(rev_dep, by = "dep", suffix=c("", ".dep")) |> 
  left_join(men_iris, by = "IRIS") |> 
  left_join(voi_iris, by = "IRIS") |> 
  mutate(sq1 = if_else(is.na(sq1.iris), if_else(is.na(sq1.com), sq1.dep, sq1.com), sq1.iris),
         sq2 = if_else(is.na(sq2.iris), if_else(is.na(sq2.com), sq2.dep, sq2.com), sq2.iris),
         sq3 = if_else(is.na(sq3.iris), if_else(is.na(sq3.com), sq3.dep, sq3.com), sq3.iris),
         sq4 = if_else(is.na(sq4.iris), if_else(is.na(sq4.com), sq4.dep, sq4.com), sq4.iris)) |> 
  select(-ends_with(c(".dep", ".iris", ".com")))

iris4mod_to <- c200ze |> 
  filter(emp>0) |> 
  mutate(commune = str_sub(IRIS, 1, 5),
         dep = str_sub(IRIS, 1, 2))

qs::qsave(iris4mod, "/space_mounts/data/marseille/iris4mod.qs")
# on vérifie
library(tmap)

tmap_mode("view")
tm_shape(iris4mod )+tm_fill(col="voiture")

tm_shape(iris4mod |> mutate(i2 = ti2/ind_iris))+tm_fill(col="i2")
tm_shape(iris4mod)+tm_fill(col="com22")
