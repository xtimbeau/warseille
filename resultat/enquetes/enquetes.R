# enquêtes

library(tidyverse)
library(ofce)

# curl::curl_download(
#   "https://www.insee.fr/fr/statistiques/fichier/6439600/grille_densite_7_niveaux_2024.xlsx",
#   destfile = "/tmp/grille7.xlsx")
# 
# grille7 <- readxl::read_xlsx("/tmp/grille7.xlsx", skip = 4) |>
#   distinct(DENS, LIBDENS) |>
#   arrange(DENS) |> 
#   pull(LIBDENS)

enq <- source_data("resample mobilite.r")

emp <- enq$emp_4$raw
emc2 <- enq$emc2_4_car$raw
emc2_7 <- enq$emc2_7$raw

ff <- function(x, n) {
  map_chr(x, ~{
    if(.x>=100)
      str_c(format(.x/1000, digits=n, scientific=FALSE , big.mark=" "), "k")
    else if(.x<100)
      format(.x, digits=n, scientific=FALSE , big.mark=" ") 
  })
}

collapse_cells <- function(data, out = "km") {
  vars <- str_c(out, c("ac", "ad", "ac_r", "ad_r"))
  data |> 
    select(-adultes, -obs) |> 
    filter(variable%in%vars) |>
    mutate(variable = str_remove(variable, out),
           grp = ifelse(str_detect(variable, "_r$"), "relatif", "km"),
           pa = str_remove(variable, "_r")) |> 
    pivot_longer(cols = c(travail, total, autres, courses, etudes)) |>
    drop_na(value) |> 
    mutate(name = str_c(name, "_", pa)) |> 
    select(-pa, -variable) |> 
    pivot_wider(names_from = name, 
                values_from = value) |> 
    relocate(vv, 
             travail_ac, 
             travail=travail_ad,  
             etudes = etudes_ad,  
             courses = courses_ad, 
             autres = autres_ad,  
             total = total_ad) |> 
    group_by(grp, vv) |> 
    summarize(
      across(-p, 
             ~ str_c(ff(.x[p==0.5], 2),
                     "<br><small><small>[",
                     ff(.x[p==0.025], 2), ", ", 
                     ff(.x[p==0.975], 2), "]</small></small>")),
      .groups = "drop")
}

tab_emp <- collapse_cells(emp) |> 
  filter((grp == "km" & vv== "total") | (grp == "relatif" & vv != "total"), vv != "très peu dense") |> 
  left_join(emp |> filter(variable=="dtrjac", p==0.5) |> select(vv, adultes, obs), by="vv") |> 
  mutate(src = "emp")

tab_emc2 <- collapse_cells(emc2) |> 
  filter((grp == "km" & vv== "total") | (grp == "relatif" & vv != "total" ), vv != "très peu dense") |> 
  left_join(emc2 |> filter(variable=="dtrjac", p==0.5) |> select(vv, adultes, obs), by="vv") |> 
  mutate(src = "emc2")

tab <- bind_rows(tab_emp, tab_emc2) |> 
  mutate(
    src = factor(src, c("emp", "emc2"), c("EMP 2019 +700k", "EMC^2^ AMP 2020") ) )

vv7 <- c("Grands centres urbains", "Centres urbains intermédiaires", "Petites villes",
        "Ceintures urbaines", "Bourgs ruraux", "Rural à habitat dispersé", "Rural à habitat très dispersé") 

tab_emc2_7 <- collapse_cells(emc2_7) |> 
  filter((grp == "km" & vv== "total") | (grp == "relatif" & vv != "total" ), vv != "très peu dense") |> 
  left_join(emc2_7 |> filter(variable=="dtrjac", p==0.5) |> select(vv, adultes, obs), by="vv") |> 
  mutate(src = "emc2", 
         vv = vv7[as.numeric(vv)] |> replace_na("total")) |> 
  mutate(
    src = factor(src, c("emp", "emc2"), c("EMP 2019", "EMC^2^ AMP 2020") ) )

return(list(emp = emp, emc2 = emc2, tab_emc2_7 = tab_emc2_7, emc2_7 = emc2_7, tab = tab))
