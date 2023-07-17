# remotes::install_github("nuvolos-cloud/r-connector")
install.packages("remotes")

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
conflict_prefer("filter",'dplyr', quiet = TRUE)
conflict_prefer("select",'dplyr', quiet = TRUE)

load("baselayer.rda") # executer 1. zones avant

#iris18 <- qread('{DVFdata}/iris18.qs' |> glue())

c200 <- qread(c200ze_file) # version 2017

# FUA <- st_read("{DVFdata}/sources/FUA/FRA core commuting/FRA_core_commuting.shp" |> glue(), 
#                stringsAsFactors=FALSE) |> st_transform(3035)
# 
# FUAcore <- st_read("{DVFdata}/sources/FUA/FRA core/FRA_core.shp" |> glue(),
#                    stringsAsFactors=FALSE) |> st_transform(3035)
# 
# LaRochelle_fua <- FUA |> filter(fuaname == "La Rochelle") |> pull(geometry)

Marseille_c200s <- accesstars::idINS2stars(c200 |> select(emp, idINS), zone_emploi)

##################################################################
# Adding information on jobs w FF2018 and FLORES (impute Flores) #
##################################################################
rm(c200) 

fua_c200s <- Marseille_c200s

# ---- source : locaux as extract from FF2018 ----
# con <- nuvolos::get_connection()
# locaux <- dbGetQuery(con, "SELECT sprincp, cconac, idcom, X, Y
#                      FROM DV3FV231_LOCAL
#                      WHERE ccodep IN ('13', '30', '80', '84') AND
#                            cconac IS NOT NULL ;")
# 
# locaux <- locaux |> 
#   mutate(NAF = str_sub(CCONAC, 1, 2)) |> 
#   rename(sp = SPRINCP)

# if(fs::dir_exists("{DVFdata}/sources/flores/2017" |> glue())) fs::dir_delete("{DVFdata}/sources/flores/2017" |> glue())
# fs::dir_create("{DVFdata}/sources/flores/2017/" |> glue())
# archive::archive_extract("https://www.insee.fr/fr/statistiques/fichier/4991205/TD_FLORES2017_NA88_NBSAL_CSV.zip", 
#                          dir = "{DVFdata}/sources/flores/2017/" |> glue())

# ---- code NAF ----
# à télécharger sur le site en mettant les labels
# naf <- readxl::read_excel(glue("{DVFdata}/sources/Flores/naf2008_5_niveaux.xls"), sheet="naf") |>
#   select(NAF=NIV2, NAF1=NIV1, NAFl1=label1, NAFl2=label2) |>
#   group_by(NAF) |>
#   summarise(across(c(NAF1, NAFl1, NAFl2), first))
# 
# naf1 <- naf |>
#   group_by(NAF1) |>
#   summarise(NAFl1=first(NAFl1)) |>
#   mutate(NAFl1 = glue("({NAF1}) {NAFl1}"))
# 
# # ---- producing a stars object w surface by NAF as attributes and x, y as dimensions ----
# surf_by_naf <- locaux |>
#   filter(sp > 0) |>
#   drop_na(NAF, X, Y) |> 
#   select(sp, NAF, X, Y) |>
#   left_join(select(naf, NAF,NAF1), by="NAF") |>
#   filter(!is.na(NAF1)) |>
#   st_as_sf(coords = c("X", "Y"), crs = 3035)
# 
# surf_by_naf <- surf_by_naf[zone_emploi, ]
# list_naf <- map_chr(surf_by_naf |> group_split(NAF1), pluck, "NAF1", 1)
# 
# fua_template <- fua_c200s |> select(1)
# fua_template[[1]][] <- 0
# 
# # Warning : st_rasterize adds count to the template, which should be set to 0 if the count starts from scratch
# # It sums only the first attribute (version stars < 0.5.3).
# # It sums each attributes (stars >= 0.5.3)
# surf_by_naf <- map(surf_by_naf |> group_split(NAF1),
#                    st_rasterize,
#                    template = fua_template,
#                    options = "MERGE_ALG=ADD")
# 
# surf_by_naf <- map2(surf_by_naf, list_naf, ~ { names(.x) <- paste0(names(.x), .y) ; .x } )
# surf_by_naf <- reduce(surf_by_naf, c)
# 
# ---- source : iris ----
iris_region <- iris18[zone_emploi, ] |>
  rename(COM=DEPCOM) |> 
  group_by(COM) |>
  summarize(DEP = first(DEP)) |>
  rename(idcom=COM)

# surfbynaf_iris <- aggregate(surf_by_naf, by = iris_region, FUN = sum, na.rm = TRUE) |>
#   st_as_sf() |>
#   st_drop_geometry()
# 
# surfbynaf_iris <- bind_cols(iris_region |> st_drop_geometry() |> select(idcom),
#                             surfbynaf_iris) |>
#   pivot_longer(cols = -c(idcom), names_to = c("sp", "sp_act"),
#                names_pattern = "sp([[:upper:]])?_?a?c?t?([[:upper:]])?",
#                values_to = "surface") |>
#   mutate(NAF1 = str_c(sp, sp_act),
#          act = ifelse(str_detect(sp_act, "[[:upper:]]"), "sp_act", "sp")) |>
#   select(idcom, NAF1, surface, act) |>
#   pivot_wider(id_cols = c(idcom, NAF1), names_from = act, values_from = surface)

# ---- source : FLORES ----
# flores <- data.table::fread("{DVFdata}/sources/flores/2017/TD_FLORES2017_NA88_NBSAL.csv" |> glue()) |>
#   pivot_longer(cols = starts_with("EFF_"), names_to = "NAF", values_to = "EMP") |>
#   mutate(NAF = str_remove(NAF, "EFF_")) |>
#   rename(idcom = CODGEO) |>
#   left_join(naf, by = "NAF") |>
#   filter(NAF != "TOT")
# 
# flores <- flores |>
#   group_by(idcom, NAF1) |>
#   summarise(EMP = sum(EMP, na.rm = TRUE))
# 


#on change la source nous ce sera mobpro.pris dans le code marge_mobpro.

# 
# 
# # ---- join surface by iris and NAF1 w EMP from flores to estimate jobs by naf and surface ----
# emp_surf_by_naf_iris <- left_join(surfbynaf_iris, flores, by = c("idcom", "NAF1"))
# 
# mod_surf_to_emp <- emp_surf_by_naf_iris |>
#   nest(data_secteur = -NAF1) |>
#   mutate(lm_res = map(data_secteur, ~ {
#     data <- filter(.x, EMP > 0, sp > 0)
#     if(nrow(data)<3) return(NA)
#     lm(log(EMP)~log(sp), data = data)}))
# 
# # ggplot(emp_surf_by_naf_iris |> filter(sp > 0, EMP > 0), aes(x = log(EMP), y = log(sp), col = NAF1)) +
# #   geom_point(alpha = 0.1) +
# #   geom_smooth(method = "lm") +
# #   facet_wrap(~NAF1) +
# #   guides(fill = FALSE, color = FALSE, alpha = FALSE)
# 
# surf_by_naf <- surf_by_naf |> select(!contains("_")) # drop sp_act
# names(surf_by_naf) <- str_sub(names(surf_by_naf), 3)
# surf_by_naf <- surf_by_naf |> select(-O, -U)
# 
# mod_surf_to_emp <- mod_surf_to_emp |>
#   filter(NAF1 != "O", NAF1!= "U") |>
#   pull(lm_res)
# 
# input_jobs <- function(x, ...) {
#   if (is.na(x) | x == 0) return(0)
#   ceiling(exp(predict(object = ..., newdata = data.frame(sp = x))))
# }
# 
# emp_pred <- imap(mod_surf_to_emp, ~ {
#   st_apply(X = surf_by_naf[.y],
#            MARGIN = c("x", "y"),
#            FUN = "input_jobs",
#            object = .x)
# })
# 
# emp_pred_tot <- reduce(emp_pred, `+`)
# names(emp_pred_tot) <- "emp_pred"
# 
# qs::qsave(emp_pred_tot, file=glue("{DVFdata}/emp33km.qs"))
# 
# # # ---- checking result and testing alternatives ----
# # emp_pred <- reduce(emp_pred, c)
# # 
# # emp_pred_by_com <- aggregate(emp_pred, by = iris_region, FUN = sum, na.rm = TRUE) |>
# #   st_as_sf() |>
# #   st_drop_geometry() |>
# #   bind_cols(iris_region |> st_drop_geometry() |> select(idcom, EMP09)) |> 
# #   pivot_longer(cols = -c(idcom, EMP09), names_to = "NAF1", values_to = "emp_pred") |>
# #   left_join(flores, by = c("idcom", "NAF1")) |>
# #   group_by(idcom, NAF1) |> 
# #   mutate(EMP_pred = sum(emp_pred, na.rm = TRUE)) |> 
# #   ungroup() |>
# #   mutate(coef_correction = ifelse(EMP_pred == 0, NA, EMP / EMP_pred),
# #          emp_pred_corrected = emp_pred * coef_correction)
# # 
# # 
# # emp_pred_by_com |> pull(coef_correction) |> summary()
# # 
# # emp_pred_by_com2 <- emp_pred_by_com |> 
# #   group_by(idcom) |> 
# #   summarise(EMP = sum(EMP, na.rm = TRUE),
# #             EMP_pred = sum(EMP_pred, na.rm = TRUE)) |> 
# #   mutate(coef_correction = ifelse(EMP_pred == 0, NA, EMP / EMP_pred))
# # 
# # emp_pred_by_com2 |> pull(coef_correction) |> summary()
# # # this alt imposes a large reallocation, too large...
# # 
# # aggregate(emp_pred_tot, by = Marseille_Aix_fua, FUN = sum, na.rm = TRUE) # 584 755 jobs in 2015
# # iris_region |> pull(EMP09) |> sum() # 493 391 jobs in 2009
# # 
# # mapview::mapview(emp_pred_tot |> mutate(emp_pred = ifelse(emp_pred == 0, NA, emp_pred)),
# #         na.color = NA)
# # 


enqmobpro <- read_xlsx("~/files/base-flux-mobilite-domicile-lieu-travail-2019.xlsx", sheet="Flux_sup_100", skip=5)

mobpro <- enqmobpro
setDT(mobpro)

# mobpro <- fread(enqmobpro)[between(AGEREVQ,18,64),]
# mobpro <- as.data.table(dpro, keep.rownames=TRUE)

communes_emplois <- c200ze$IRIS


mobpro[, filter_live := CODGEO %in% scot_tot.epci] #faut-il mettre ce ?
mobpro[, filter_work := DCLT %in% communes_emplois]


mobpro <- mobpro[!(filter_live == FALSE & filter_work == FALSE)]

mobpro <- mobpro |> 
  rename(COMMUNE = CODGEO) 

# ---- Synthèse MOBPRO par codes NAF et modes de transport ----
mobilites <- mobpro[, .(NB = sum(IPONDI)), by = c("COMMUNE", "DCLT", "NA5", "TRANS")]
mobilites[, TRANS := factor(TRANS) |> 
            fct_recode("none" = "1","walk" = "2", "bike" = "3", "car" = "4", "transit" = "5")]

emplois_by_DCLT <- mobilites[, .(nb_emplois = sum(NB)), by = c("DCLT", "NA5")]

# ---- Récupération des surfaces de FF2018 par code NAF ----
con <- nuvolos::get_connection()
locaux <- DBI::dbGetQuery(con, "SELECT sprincp, cconac, stoth, slocal, idcom, X, Y
                     FROM FF2018_PB0010_LOCAL
                     WHERE ccodep IN ('13', '30', '80', '84') AND
                           (cconac IS NOT NULL OR stoth > 0) ;")
locaux_h <- locaux |> 
  filter(is.na(CCONAC)) |> 
  rename(sp = SPRINCP,
         sh = STOTH,
         slocal = SLOCAL) |> 
  mutate(ts = ifelse(sp!=0, sp,
                     ifelse(sh!=0, sh/2, slocal/4)))

locaux <- locaux |> 
  filter(!is.na(CCONAC)) |> 
  mutate(NAF = str_sub(CCONAC, 1, 2)) |> 
  rename(sp = SPRINCP,
         sh = STOTH,
         slocal = SLOCAL) |> 
  mutate(ts = ifelse(sp!=0, sp,
                     ifelse(sh!=0, sh/2, slocal/4)))

naf <- readxl::read_excel(glue("~/files/naf2008_5_niveaux.xls")) |>
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
  filter(ts > 0, IDCOM %in% com_ze$insee) |>
  drop_na(group_naf, X, Y) |> 
  select(ts, group_naf, X, Y, IDCOM) |>
  st_as_sf(coords = c("X", "Y"), crs = 3035)

# c200ze <- c200.scot_tot |> filter(com %in% com_ze$insee) #pk je n'ai pas de commune??? on pourrait faire avec les polynomes mais c chiant

template_lr <- c200ze |> select(idINS) |> accesstars::idINS2stars()
template_lr[[1]][] <- 0

surf_by_naf.st <- map(surf_by_naf |> select(ts, group_naf) |> group_split(group_naf), \(x) {
  res <- st_rasterize(x, template = template_lr, options = "MERGE_ALG=ADD")
  names(res) <- x$group_naf[1]
  return(res)
}) |> 
  reduce(c) 
# on rasterize les _h


surf_by_naf_h <- locaux_h |>
  filter(ts > 0, IDCOM %in% communes_emplois) |>
  drop_na(X, Y) |> 
  select(ts, X, Y, IDCOM) |>
  st_as_sf(coords = c("X", "Y"), crs = 3035)

template_lr <- c200ze |> select(idINS) |> accesstars::idINS2stars()
template_lr[[1]][] <- 0

surf_by_naf_h.st <- st_rasterize(surf_by_naf_h, template = template_lr, options = "MERGE_ALG=ADD")

# ---- Surfaces totales par NAF et commune ----
communes.st <- c200ze |> 
  transmute(idINS,
            DCLT = str_sub(IRIS, 1, 5) |> as.integer()) |> # rasterize trompeur sur char
  accesstars::idINS2stars()

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
verif[, sum(nb_emplois)] / emplois_by_DCLT[, sum(nb_emplois)]

emplois_by_DCLT_c <- emplois_by_DCLT |> 
  left_join( surf_by_naf_DCLT, by = c("DCLT", "NA5")) |> 
  filter(DCLT %chin% communes_emplois) |> 
  group_by(DCLT) |> 
  mutate(zut = all(is.na(ts)),
         orphelins = sum(nb_emplois[is.na(ts)]),
         nb_emplois_c = nb_emplois + orphelins*nb_emplois/sum(nb_emplois[!is.na(ts)]),
         nb_emplois_c = ifelse(is.na(ts)&!zut, 0,
                               ifelse(zut, 0 , nb_emplois_c))) |> 
  ungroup() 

emp_pred <- c(surf_by_naf.st, communes.st) |> 
  as_tibble() |>
  pivot_longer(cols = -c(DCLT, x, y), names_to = "NA5", values_to = "ts") |> 
  filter(DCLT != 0, ts != 0) |> 
  inner_join(emplois_by_DCLT_c |> mutate(DCLT=as.numeric(DCLT)), by=c("DCLT", "NA5"), suffix = c("", ".com")) |> 
  mutate(emp_pred = ts/ts.com * nb_emplois_c) |> 
  select(x,y,emp_pred) |>
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

mobilites[, DCLT := fifelse(DCLT %chin% communes_emplois, DCLT, "Hors zone")]
mobilites[, COMMUNE := fifelse(COMMUNE %chin% communes_residents, COMMUNE, "Hors zone")]

mobilites <- mobilites[COMMUNE != "Hors zone"] # ici on n'a pas besoin des travailleurs non résidents.

mobilites[, tot_navetteurs := walk + bike + transit + car]

# ---- Estimation des marges des actifs résidents ----
popact <- readxl::read_xlsx("{popactive_file}" |> glue(), sheet = "COM_2018", skip=5) |>
  transmute(COMMUNE=CODGEO,
            pop1564 = P18_POP1564,
            act1564 = P18_ACT1564,
            chom1564 = P18_CHOM1564) 

les_actifs <- c200ze |> 
  mutate(COMMUNE = str_sub(CODE_IRIS, 1, 5)) |> 
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