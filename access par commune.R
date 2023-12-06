library(arrow)
library(sf)
library(r3035)
library(data.table)
library(conflicted)
library(tidyverse)
library(MetricsWeighted)

load("baselayer.rda")
data.table::setDTthreads(12)
arrow::set_cpu_count(16)

conflict_prefer("dt2r", "r3035")

modes <- set_names(c("walk_tblr"))
modes <- set_names(c("bike_tblr"))
modes <- set_names(c("car_dgr2"))
modes <- set_names(c("transit5"))


data <- map( modes, ~ arrow::open_dataset("/space_mounts/data/marseille/distances/src/{.x}" |> glue()) |>
               select(fromId, toId, travel_time, COMMUNE, DCLT) |>
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


conflict_prefer("filter", "dplyr")
conflict_prefer("select", "dplyr")
c200ze <- qs::qread(c200ze_file)
com21 <- qs::qread(com2021_file)
setDT(c200ze)
times <- seq(1, 120, 1)
times <- set_names(times, str_c("t", times))
seuils <- seq(1000, 150000, length.out = 50)

access <- imap(data, ~{
  ld <- .x |>
    merge( 
      c200ze[emp>0, .(toidINS=idINS, emp, geometry)],
      by=c("toidINS"), all.y=TRUE) |> 
    drop_na(fromidINS) |> 
    merge(
      c200ze[scot==TRUE, .(fromidINS=idINS, com22)],
      by="fromidINS", all.x = TRUE)
  setDT(ld)
  res <- ld[, 
            lapply(times, \(.x) sum(emp*(travel_time_transit<=.x), na.rm=TRUE)), 
            by="fromidINS"]
  res <- accessibility::iso2time(dt2r(res), seuils = seuils) 
  res <- r2dt(res)
  res[, `:=`(x=NULL, y=NULL, mode=.y)]
  res
}) |> 
  bind_rows() 


access <- access |>
  as_tibble() |> 
  rename(fromidINS = idINS200) |> 
  left_join(c200ze |> select(com22, fromidINS = idINS, ind), by="fromidINS") |> 
  pivot_longer(cols = starts_with("to"), names_to = "emp", values_to = "temps") |> 
  mutate(emp = as.numeric(str_remove(str_remove(emp, "to"), "k"))*1000)

fromCom <- access |> distinct(com22) |> pull()

# calcul d'accessibilité à l'emploi par modes --------------
library(accessibility)
library(MetricsWeighted)

mode_l <- access |> 
  group_by(com22,  mode, emp) |> 
  filter(!is.na(temps)) |> 
  summarize(temps = weighted_median(temps, w = ind),
            repr = n()) |> 
  filter(repr>15)

tout <- access |> 
  group_by(emp, mode) |> 
  filter(!is.na(temps)) |> 
  summarize(temps = weighted_median(temps, w = ind),
            repr = n()) |> 
  mutate(com22="99999") |> 
  filter(repr>15)

mode_l <- bind_rows(mode_l, tout) |> ungroup() |> 
  mutate(label = case_when(
    com22 == 13055 ~ 'Marseille',
    com22 == 06088 ~ 'Nice',
    com22 == 83137 ~ 'Toulon',
    com22 == 13001 ~ 'Aix-en-Provence',
    com22 == 99999 ~ 'Tout le territoire',
    TRUE ~ str_c("n", com22)),
    label = factor(
      label, 
      levels = c("Tout le territoire", "Marseille", "Nice",
                 "Toulon", "Aix-en-Provence")),
    mode = factor(
      mode, 
      c("car", "transit", "bike", "walk"), 
      c("Voiture", "Transport en commun", "Vélo", "Marche à pied")))

qs::qsave(mode_l, "output/model_l_car.sqs")
library(scales)
access_par_com_bike <- ggplot(mode_l) +
  geom_line(aes(x=temps, y=emp, group=com22), col="gray80", linewidth=0.2) +
  geom_line(data = ~filter(.x, !str_detect(label, "^n")),
            aes(x=temps, y=emp, color=label)) +
  scale_x_continuous(breaks  = c(0, 20,40,60,80,100,120))+
  scale_y_continuous(labels = ofce::f2si2, breaks = c(25000, 50000, 75000, 100000))+
  ofce::theme_ofce(base_family = "Roboto")+
  xlab("temps en minutes") +
  labs(color="Communes")+
  theme(legend.position = c(0.01, 0.99),
        legend.justification = c(0,1),
        panel.spacing = unit(12, "pt"),
        plot.margin = margin(l = 6, r= 6),
        panel.grid.major.x = element_line(color="gray80", linewidth = 0.1))+
  facet_wrap(vars(mode))

ofce::graph2png(access_par_com_transit, rep = output_rep, dpi=600, ratio = 4/3)

#pour faire car_dgr2

data <- arrow::open_dataset("/space_mounts/data/marseille/distances/src/car_dgr2") |>
  to_duckdb() |>
  group_by(COMMUNE) |>
  merge(c200ze[emp>0, .(toidINS=idINS, emp, geometry)], by=c("toidINS"), all.y=TRUE) |> 
  drop_na(fromidINS) |> 
  merge(c200ze[scot==TRUE, .(fromidINS=idINS, com22)], by="fromidINS", all.x = TRUE) |>
  collect() |>
  mutate(COMMUNE = as.character(COMMUNE)) 
  
  

setDT(data)
access <- imap(data, ~ {
  
res <- data[, 
          lapply(times, \(.x) sum(emp*(travel_time_transit<=.x), na.rm=TRUE)), 
          by="fromidINS"]
res <- accessibility::iso2time(dt2r(res), seuils = seuils) 
res <- r2dt(res)
res[, `:=`(x=NULL, y=NULL, mode=.y)]
res
}) |> bind_rows()


access <- access |>
  as_tibble() |> 
  rename(fromidINS = idINS200) |> 
  left_join(c200ze |> select(com22, fromidINS = idINS, ind), by="fromidINS") |> 
  pivot_longer(cols = starts_with("to"), names_to = "emp", values_to = "temps") |> 
  mutate(emp = as.numeric(str_remove(str_remove(emp, "to"), "k"))*1000)

fromCom <- access |> distinct(com22) |> pull()

mode_l <- access |> 
  group_by(com22,  mode, emp) |> 
  filter(!is.na(temps)) |> 
  summarize(temps = weighted_median(temps, w = ind),
            repr = n()) |> 
  filter(repr>15)

tout <- access |> 
  group_by(emp, mode) |> 
  filter(!is.na(temps)) |> 
  summarize(temps = weighted_median(temps, w = ind),
            repr = n()) |> 
  mutate(com22="99999") |> 
  filter(repr>15)

mode_l <- bind_rows(mode_l, tout) |> ungroup() |> 
  mutate(label = case_when(
    com22 == 13055 ~ 'Marseille',
    com22 == 06088 ~ 'Nice',
    com22 == 83137 ~ 'Toulon',
    com22 == 13001 ~ 'Aix-en-Provence',
    com22 == 99999 ~ 'Tout le territoire',
    TRUE ~ str_c("n", com22)),
    label = factor(
      label, 
      levels = c("Tout le territoire", "Marseille", "Nice",
                 "Toulon", "Aix-en-Provence")),
    mode = factor(
      mode, 
      c("car", "transit", "bike", "walk"), 
      c("Voiture", "Transport en commun", "Vélo", "Marche à pied")))

access_par_com_car_dgr2 <- ggplot(mode_l) +
  geom_line(aes(x=temps, y=emp, group=com22), col="gray80", linewidth=0.2) +
  geom_line(data = ~filter(.x, !str_detect(label, "^n")),
            aes(x=temps, y=emp, color=label)) +
  scale_x_continuous(breaks  = c(0, 20,40,60,80,100,120))+
  scale_y_continuous(labels = ofce::f2si2, breaks = c(25000, 50000, 75000, 100000))+
  ofce::theme_ofce(base_family = "Roboto")+
  xlab("temps en minutes") +
  labs(color="Communes")+
  theme(legend.position = c(0.01, 0.99),
        legend.justification = c(0,1),
        panel.spacing = unit(12, "pt"),
        plot.margin = margin(l = 6, r= 6),
        panel.grid.major.x = element_line(color="gray80", linewidth = 0.1))+
  facet_wrap(vars(mode))






