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
conflict_prefer_all("dplyr", quiet = TRUE)
source("secrets/azure.R")

c200ze <- qs::qread(c200ze_file)
bd_write(c200ze)
times <- seq(1, 120, 1)
times <- set_names(times, str_c("t", times))
seuils <- c(10000, 20000, 50000, 100000, 200000, 300000, 4000000, 500000)

modes <- set_names(c("walk_tblr", "bike_tblr",
                     'transit', 'transit5',"car_dgr"))
emploi <- c200ze |> 
  st_drop_geometry() |> 
  filter(emp>0) |>
  select(toidINS=idINS, emp, ind) |> 
  write_dataset(("/tmp/emploi"))

emploi <- open_dataset("/tmp/emploi") |> 
  to_duckdb()

temps <- 1:120
txt <- str_c("emp", temps, "= sum(emp*as.numeric(travel_time<=", temps,"), na.rm=TRUE)") 
unlink("/tmp/access.r")
ff <- file("/tmp/access.r", open = "at")
writeLines("dd <- dd |> summarise(", ff)
walk(head(txt, -1), ~writeLines(str_c(.x, ","), ff))
writeLines(str_c(dplyr::last(txt), ")"), ff)
close(ff)

access <- map_dfr(
  modes, ~{
    dd <- arrow::open_dataset("/space_mounts/data/marseille/distances/src/{.x}" |> glue()) |>
      to_duckdb()
    
    if(str_detect(.x, "transit") )
      dd <- dd |>
        select(fromidINS, toidINS, travel_time, COMMUNE, DCLT)
    else {
      if(str_detect(.x, "car") )
        dd <- dd |>
          select(fromidINS=fromId, toidINS=toId, travel_time=travel_time_park, COMMUNE, DCLT)
      else 
        dd <- dd |>
          select(fromidINS=fromId, toidINS=toId, travel_time, COMMUNE, DCLT)
      
    } 
    dd <- dd |> 
      mutate(COMMUNE = as.character(COMMUNE)) |> 
      left_join(emploi, by="toidINS") |>
      filter(!is.na(fromidINS) ) |> 
      filter(!is.na(travel_time)) |> 
      group_by(fromidINS)
    source("/tmp/access.r", local = TRUE)
    dd |> 
      collect() |> 
      mutate(mode = .x)
  }, .progress=TRUE)

unlink("space_mounts/data/marseille/distances/access", recursive =TRUE)
arrow::write_dataset(access, "/space_mounts/data/marseille/distances/access")
access <- arrow::open_dataset("/space_mounts/data/marseille/distances/access") |> 
  to_duckdb()

t_access <- imap(modes, ~{
  res <- access |> filter(mode== .x) |> select(-mode) |> collect()
  res <- accessibility::iso2time(dt2r(res), seuils = seuils) 
  res <- r2dt(res)
  res[, `:=`(x=NULL, y=NULL, mode=.x)]
  res
}) |> 
  bind_rows() 

t_access <- t_access |>
  st_drop_geometry() |> 
  left_join(c200ze |> select(idINS200=idINS, com, ind), by="idINS200") |> 
  mutate(
    geometry=idINS2square(idINS200),
    mode_lib = factor(
      mode, 
      c("car_dgr2", "transit5", "transit", 
        "bike_tblr", "bike_ntblr", "walk_tblr", "walk_ntblr"), 
      c("Voiture", "Transport en commun (q5%)", "Transport en commun (median)", 
        "Vélo", "Vélo (sans tblr)", "Marche à pied", "Marche à pied (sans tblr"))) |> 
  st_as_sf(crs=3035)

bd_write(t_access)
write_csv(t_access |> st_drop_geometry(), file="output/access.csv")
qs::qsave(t_access, "output/acces4modes.sqs")
load(decor_carte_file)
qs::qsave(decor_carte, "output/decor_carte.sqs")
bd_write(decor_carte)
(access_4modes_walk <- ggplot()+
    decor_carte +
    ofce::theme_ofce_void(axis.text = element_blank()) +
    geom_sf(data=t_access , aes(fill=to50k), col=NA)+
    scico::scale_fill_scico(palette="hawaii", na.value=NA, direction=-1, name = "mn")+
    annotation_scale(line_width = 0.2, height = unit(0.1, "cm"), 
                     text_cex = 0.4, pad_y = unit(0.1, "cm"))+
    facet_wrap(vars(mode)))

# accessibilité par communes

c_access <- access |>
  collect() |> 
  rename(idINS = fromidINS) |> 
  left_join(c200ze |> select(com, idINS, ind), by="idINS") |> 
  pivot_longer(cols = starts_with("emp"), names_to = "temps", values_to = "emp") |> 
  mutate(temps = as.numeric(str_remove(temps, "emp")))

fromCom <- c_access |> distinct(com) |> pull()

# calcul d'accessibilité à l'emploi par modes --------------
library(MetricsWeighted)

mode_l <- c_access |> 
  group_by(com,  mode, temps) |> 
  summarize(emp = weighted_median(emp, w = ind),
            repr = n(), 
            .groups = "drop")

tout <- c_access |> 
  group_by(temps, mode) |> 
  summarize(emp = weighted_median(emp, w = ind),
            repr = n(), 
            .groups = "drop") |> 
  mutate(com="99999")

access_par_com <- bind_rows(mode_l, tout) |> 
  ungroup() |> 
  mutate(label = case_when(
    com == 13203 ~ 'Marseille 3e',
    com == 13207 ~ 'Marseille 7e',
    com == 13039  ~ 'Fos-sur-Mer',
    com == 13001 ~ 'Aix-en-Provence',
    com == 99999 ~ 'Tout le territoire',
    TRUE ~ str_c("n", com)),
    label = factor(
      label, 
      levels = 
        c("Tout le territoire",
          "Marseille 3e", "Marseille 7e",
          "Fos-sur-Mer", "Aix-en-Provence")))

bd_write(access_par_com)

access_par_com_g <- ggplot(
  access_par_com |> 
    filter(mode %in% c("car_dgr2", "transit5", "bike_tblr", "walk_tblr"))) +
  geom_line(aes(x=temps, y=emp, group=com), col="gray80", linewidth=0.2) +
  geom_line(data = ~filter(.x, !str_detect(label, "^n")),
            aes(x=temps, y=emp, color=label)) +
  scale_x_continuous(breaks  = c(0,20,40,60,80,100,120), limits = c(0,60))+
  scale_y_continuous(labels = ofce::f2si2)+
  ofce::theme_ofce()+
  xlab("temps en minutes") +
  labs(color="Communes")+
  theme(legend.position = c(0.01, 0.99),
        legend.justification = c(0,1),
        panel.spacing = unit(12, "pt"),
        plot.margin = margin(l = 6, r= 6),
        panel.grid.major.x = element_line(color="gray80", linewidth = 0.1))+
  facet_wrap(vars(mode))

access_par_com_btblr <- ggplot(
  access_par_com |> 
    filter(mode %in% c("bike_tblr", "bike_ntblr"))) +
  geom_line(aes(x=temps, y=emp, group=com), col="gray80", linewidth=0.2) +
  geom_line(data = ~filter(.x, !str_detect(label, "^n")),
            aes(x=temps, y=emp, color=label)) +
  scale_x_continuous(breaks  = c(0, 20,40,60,80,100,120), limits = c(0,60))+
  scale_y_continuous(labels = ofce::f2si2)+
  ofce::theme_ofce()+
  xlab("temps en minutes") +
  labs(color="Communes")+
  theme(legend.position = c(0.01, 0.99),
        legend.justification = c(0,1),
        panel.spacing = unit(12, "pt"),
        plot.margin = margin(l = 6, r= 6),
        panel.grid.major.x = element_line(color="gray80", linewidth = 0.1))+
  facet_wrap(vars(mode))

access_par_com_wtblr <- ggplot(
  access_par_com |> 
    filter(mode %in% c("walk_tblr", "walk_ntblr"))) +
  geom_line(aes(x=temps, y=emp, group=com), col="gray80", linewidth=0.2) +
  geom_line(data = ~filter(.x, !str_detect(label, "^n")),
            aes(x=temps, y=emp, color=label)) +
  scale_x_continuous(breaks  = c(0, 20,40,60,80,100,120))+
  scale_y_continuous(labels = ofce::f2si2)+
  ofce::theme_ofce()+
  xlab("temps en minutes") +
  labs(color="Communes")+
  theme(legend.position = c(0.01, 0.99),
        legend.justification = c(0,1),
        panel.spacing = unit(12, "pt"),
        plot.margin = margin(l = 6, r= 6),
        panel.grid.major.x = element_line(color="gray80", linewidth = 0.1))+
  facet_wrap(vars(mode))

# distribution de l'accessibilité
c200ze <- bd_read("c200ze")
t_access <- bd_read("t_access")
t_access  <- t_access |> 
  mutate(zone = case_match(
    com,
    str_c(13201:13216) ~ "Marseille",
    "13039"  ~ 'Fos-sur-Mer',
    "13001" ~ 'Aix-en-Provence', 
    .default = "autres"))

ggplot(t_access |> filter(mode!="transit"))+
  geom_density(aes(x=to10k, weight=ind , fill = zone, col= zone, y = after_stat(count) ), alpha=.5, position="stack") + 
  scale_color_manual(
    aesthetics = c("color", "fill"), 
    values = c("Marseille" = "chartreuse2", "Aix-en-Provence"="darkorchid1", "Fos-sur-Mer"="blue3", "autres" = "grey") ) +
  facet_wrap(vars(mode)) +
  theme_ofce()

