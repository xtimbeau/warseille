## SKIP TO MAP---------------
library(arrow)
library(tmap)
library(sf)
library(r3035)
library(data.table)
library(ofce)
library(tidyverse)
library(conflicted)
library(ggspatial)
library(scico)
load("baselayer.rda")
conflict_prefer( "dt2r", "r3035")
conflict_prefer( "r2dt", "r3035")
conflict_prefer( "idINS2square", "r3035")
conflict_prefer("filter", "dplyr")
conflict_prefer("select", "dplyr")
c200ze <- qs::qread(c200ze_file)
setDT(c200ze)
times <- seq(1, 120, 1)
times <- set_names(times, str_c("t", times))
seuils <- c(1000, 2000, 5000, 10000, 20000, 25000, 50000, 100000)

modes <- set_names(c("walk_tblr"))
modes <- set_names(c("bike_tblr"))
modes <- set_names(c("car_dgr2"))


data <- map(modes, ~ arrow::open_dataset("/space_mounts/data/marseille/distances/src/{modes}" |> glue()) |>
    # select(fromId, toId, travel_time, COMMUNE, DCLT) |>
    rename(travel_time_transit = travel_time) |>
    rename(fromidINS=fromId) |>
    rename(toidINS=toId) |>
    # filter(distance <= 25000 ) |>
    # distinct(COMMUNE, DCLT) |>
    collect() |>
    mutate(COMMUNE = as.character(COMMUNE)))

#pour transit
data <- map( modes, ~ arrow::open_dataset("/space_mounts/data/marseille/distances/src/{.x}" |> glue()) |>
               select(fromidINS, toidINS, travel_time, COMMUNE, DCLT) |>
               rename(travel_time_transit = travel_time) |>
               # filter(distance <= 25000 ) |>
               # distinct(COMMUNE, DCLT) |>
               collect() |>
               mutate(COMMUNE = as.character(COMMUNE)))

access <- imap(data, ~{
  ld <- merge(.x, 
              c200ze[emp>0, .(toidINS=idINS, emp, geometry)],
              by=c("toidINS"), all.y=TRUE) %>%
    drop_na(fromidINS) %>% 
    st_as_sf(crs=3035)
  setDT(ld)
  res <- ld[ , lapply(times, \(.x) sum(emp*(travel_time_transit<=.x), na.rm=TRUE)), by="fromidINS"]
  res <- accessibility::iso2time(dt2r(res), seuils = seuils) 
  res <- r2dt(res)
  res[, `:=`(x=NULL, y=NULL, mode=.y)]
  res
}) |> 
  bind_rows() 

access <- access |> 
  mutate(geometry=idINS2square(idINS200),
         mode = factor(mode, 
                       c("car", "transit", "bike", "walk"), 
                       c("Voiture", "Transport en commun", "Vélo", "Marche à pied"))) |> 
  st_as_sf(crs=3035)

# write_csv(access |> st_drop_geometry(), file="output/access.csv")
qs::qsave(access, "output/acces4modes.sqs")
qs::qsave(decor_carte, "output/decor_carte.sqs")

(access_4modes_walk <- ggplot()+
    decor_carte +
    ofce::theme_ofce_void(base_family = "Roboto", axis.text = element_blank()) +
    geom_sf(data=access, aes(fill=to10k), col=NA)+
    scico::scale_fill_scico(palette="hawaii", na.value=NA, direction=-1, name = "mn")+
    annotation_scale(line_width = 0.2, height = unit(0.1, "cm"), 
                     text_cex = 0.4, pad_y = unit(0.1, "cm"))+
    facet_wrap(vars(mode)))

ofce::graph2png(access_4modes_walk, rep=output_rep)

# ggplot()+
#   decor_carte +
#   geom_sf(data=diff, aes(fill=to5k), col="white", linewidth=0.01)+
#   geom_sf(data=stops.sf, aes(), col="red", size=0.25, alpha=0.75,show.legend = FALSE) +
#   scico::scale_fill_scico(palette="vik", na.value=NA, midpoint=0)