# remotes::install_github("nuvolos-cloud/r-connector")
library(qs)
library(sf)
library(glue)
library(stars)
library(tidyverse)
library(conflicted)
library(nuvolos)
library(tmap)
conflict_prefer("filter",'dplyr', quiet = TRUE)
conflict_prefer("select",'dplyr', quiet = TRUE)

load("baselayer.rda") # executer 1. zones avant

iris18 <- qread('{DVFdata}/iris18.qs' |> glue())

c200 <- qread(c200ze_file) # version 2017

# FUA <- st_read("{DVFdata}/sources/FUA/FRA core commuting/FRA_core_commuting.shp" |> glue(), 
#                stringsAsFactors=FALSE) |> st_transform(3035)
# 
# FUAcore <- st_read("{DVFdata}/sources/FUA/FRA core/FRA_core.shp" |> glue(),
#                    stringsAsFactors=FALSE) |> st_transform(3035)
# 
# LaRochelle_fua <- FUA |> filter(fuaname == "La Rochelle") |> pull(geometry)

LaRochelle_c200s <- accesstars::idINS2stars(c200 |> select(emp, idINS), zone_emploi)

##################################################################
# Adding information on jobs w FF2018 and FLORES (impute Flores) #
##################################################################
rm(c200) 

fua_c200s <- LaRochelle_c200s

# ---- source : locaux as extract from FF2018 ----
con <- nuvolos::get_connection()
locaux <- dbGetQuery(con, "SELECT sprincp, cconac, idcom, X, Y
                     FROM FF2018_PB0010_LOCAL
                     WHERE ccodep IN ('16', '17', '33', '79', '85', '86') AND
                           cconac IS NOT NULL ;")

locaux <- locaux |> 
  mutate(NAF = str_sub(CCONAC, 1, 2)) |> 
  rename(sp = SPRINCP)

# if(fs::dir_exists("{DVFdata}/sources/flores/2017" |> glue())) fs::dir_delete("{DVFdata}/sources/flores/2017" |> glue())
# fs::dir_create("{DVFdata}/sources/flores/2017/" |> glue())
# archive::archive_extract("https://www.insee.fr/fr/statistiques/fichier/4991205/TD_FLORES2017_NA88_NBSAL_CSV.zip", 
#                          dir = "{DVFdata}/sources/flores/2017/" |> glue())

# ---- code NAF ----
# à télécharger sur le site en mettant les labels
naf <- readxl::read_excel(glue("{DVFdata}/sources/Flores/naf2008_5_niveaux.xls"), sheet="naf") |>
  select(NAF=NIV2, NAF1=NIV1, NAFl1=label1, NAFl2=label2) |>
  group_by(NAF) |>
  summarise(across(c(NAF1, NAFl1, NAFl2), first))

naf1 <- naf |>
  group_by(NAF1) |>
  summarise(NAFl1=first(NAFl1)) |>
  mutate(NAFl1 = glue("({NAF1}) {NAFl1}"))

# ---- producing a stars object w surface by NAF as attributes and x, y as dimensions ----
surf_by_naf <- locaux |>
  filter(sp > 0) |>
  drop_na(NAF, X, Y) |> 
  select(sp, NAF, X, Y) |>
  left_join(select(naf, NAF,NAF1), by="NAF") |>
  filter(!is.na(NAF1)) |>
  st_as_sf(coords = c("X", "Y"), crs = 3035)

surf_by_naf <- surf_by_naf[zone_emploi, ]
list_naf <- map_chr(surf_by_naf |> group_split(NAF1), pluck, "NAF1", 1)

fua_template <- fua_c200s |> select(1)
fua_template[[1]][] <- 0

# Warning : st_rasterize adds count to the template, which should be set to 0 if the count starts from scratch
# It sums only the first attribute (version stars < 0.5.3).
# It sums each attributes (stars >= 0.5.3)
surf_by_naf <- map(surf_by_naf |> group_split(NAF1),
                   st_rasterize,
                   template = fua_template,
                   options = "MERGE_ALG=ADD")

surf_by_naf <- map2(surf_by_naf, list_naf, ~ { names(.x) <- paste0(names(.x), .y) ; .x } )
surf_by_naf <- reduce(surf_by_naf, c)

# ---- source : iris ----
iris_region <- iris18[zone_emploi, ] |>
  rename(COM=DEPCOM) |> 
  group_by(COM) |>
  summarize(DEP = first(DEP)) |>
  rename(idcom=COM)

surfbynaf_iris <- aggregate(surf_by_naf, by = iris_region, FUN = sum, na.rm = TRUE) |>
  st_as_sf() |>
  st_drop_geometry()

surfbynaf_iris <- bind_cols(iris_region |> st_drop_geometry() |> select(idcom),
                            surfbynaf_iris) |>
  pivot_longer(cols = -c(idcom), names_to = c("sp", "sp_act"),
               names_pattern = "sp([[:upper:]])?_?a?c?t?([[:upper:]])?",
               values_to = "surface") |>
  mutate(NAF1 = str_c(sp, sp_act),
         act = ifelse(str_detect(sp_act, "[[:upper:]]"), "sp_act", "sp")) |>
  select(idcom, NAF1, surface, act) |>
  pivot_wider(id_cols = c(idcom, NAF1), names_from = act, values_from = surface)

# ---- source : FLORES ----
flores <- data.table::fread("{DVFdata}/sources/flores/2017/TD_FLORES2017_NA88_NBSAL.csv" |> glue()) |>
  pivot_longer(cols = starts_with("EFF_"), names_to = "NAF", values_to = "EMP") |>
  mutate(NAF = str_remove(NAF, "EFF_")) |>
  rename(idcom = CODGEO) |>
  left_join(naf, by = "NAF") |>
  filter(NAF != "TOT")

flores <- flores |>
  group_by(idcom, NAF1) |>
  summarise(EMP = sum(EMP, na.rm = TRUE))

# ---- join surface by iris and NAF1 w EMP from flores to estimate jobs by naf and surface ----
emp_surf_by_naf_iris <- left_join(surfbynaf_iris, flores, by = c("idcom", "NAF1"))

mod_surf_to_emp <- emp_surf_by_naf_iris |>
  nest(data_secteur = -NAF1) |>
  mutate(lm_res = map(data_secteur, ~ {
    data <- filter(.x, EMP > 0, sp > 0)
    if(nrow(data)<3) return(NA)
    lm(log(EMP)~log(sp), data = data)}))

# ggplot(emp_surf_by_naf_iris |> filter(sp > 0, EMP > 0), aes(x = log(EMP), y = log(sp), col = NAF1)) +
#   geom_point(alpha = 0.1) +
#   geom_smooth(method = "lm") +
#   facet_wrap(~NAF1) +
#   guides(fill = FALSE, color = FALSE, alpha = FALSE)

surf_by_naf <- surf_by_naf |> select(!contains("_")) # drop sp_act
names(surf_by_naf) <- str_sub(names(surf_by_naf), 3)
surf_by_naf <- surf_by_naf |> select(-O, -U)

mod_surf_to_emp <- mod_surf_to_emp |>
  filter(NAF1 != "O", NAF1!= "U") |>
  pull(lm_res)

input_jobs <- function(x, ...) {
  if (is.na(x) | x == 0) return(0)
  ceiling(exp(predict(object = ..., newdata = data.frame(sp = x))))
}

emp_pred <- imap(mod_surf_to_emp, ~ {
  st_apply(X = surf_by_naf[.y],
           MARGIN = c("x", "y"),
           FUN = "input_jobs",
           object = .x)
})

emp_pred_tot <- reduce(emp_pred, `+`)
names(emp_pred_tot) <- "emp_pred"

qs::qsave(emp_pred_tot, file=glue("{DVFdata}/emp33km.qs"))

# # ---- checking result and testing alternatives ----
# emp_pred <- reduce(emp_pred, c)
# 
# emp_pred_by_com <- aggregate(emp_pred, by = iris_region, FUN = sum, na.rm = TRUE) |>
#   st_as_sf() |>
#   st_drop_geometry() |>
#   bind_cols(iris_region |> st_drop_geometry() |> select(idcom, EMP09)) |> 
#   pivot_longer(cols = -c(idcom, EMP09), names_to = "NAF1", values_to = "emp_pred") |>
#   left_join(flores, by = c("idcom", "NAF1")) |>
#   group_by(idcom, NAF1) |> 
#   mutate(EMP_pred = sum(emp_pred, na.rm = TRUE)) |> 
#   ungroup() |>
#   mutate(coef_correction = ifelse(EMP_pred == 0, NA, EMP / EMP_pred),
#          emp_pred_corrected = emp_pred * coef_correction)
# 
# 
# emp_pred_by_com |> pull(coef_correction) |> summary()
# 
# emp_pred_by_com2 <- emp_pred_by_com |> 
#   group_by(idcom) |> 
#   summarise(EMP = sum(EMP, na.rm = TRUE),
#             EMP_pred = sum(EMP_pred, na.rm = TRUE)) |> 
#   mutate(coef_correction = ifelse(EMP_pred == 0, NA, EMP / EMP_pred))
# 
# emp_pred_by_com2 |> pull(coef_correction) |> summary()
# # this alt imposes a large reallocation, too large...
# 
# aggregate(emp_pred_tot, by = Marseille_Aix_fua, FUN = sum, na.rm = TRUE) # 584 755 jobs in 2015
# iris_region |> pull(EMP09) |> sum() # 493 391 jobs in 2009
# 
# mapview::mapview(emp_pred_tot |> mutate(emp_pred = ifelse(emp_pred == 0, NA, emp_pred)),
#         na.color = NA)
# 