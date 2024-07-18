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
    com22 == 13203 ~ 'Marseille 3ème arrondissement',
    com22 == 13207 ~ 'Marseille 7ème arrondissement',
    com22 == 13039  ~ 'Fos-sur-Mer',
    com22 == 13001 ~ 'Aix-en-Provence',
    com22 == 99999 ~ 'Tout le territoire',
    TRUE ~ str_c("n", com22)),
    label = factor(
      label, 
      levels = c("Tout le territoire", "Marseille 3ème arrondissement", "Marseille 7ème arrondissement",
                 "Fos-sur-Mer", "Aix-en-Provence")),
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

emploi <- c200ze |> filter(emp>0) |> select(toidINS=idINS, emp) |> 
  to_duckdb()
communes <- c200ze |> filter(scot==TRUE) |> select(fromidINS=idINS, com22) |> 
  to_duckdb()


data <- arrow::open_dataset("/space_mounts/data/marseille/distances/src/car_dgr2") |>
  to_duckdb() |>
  select(fromId, toId, travel_time, COMMUNE, DCLT) |>
  rename(fromidINS=fromId) |>
  rename(toidINS=toId) |>
  left_join(emploi, by=c("toidINS")) |> 
  filter(!is.na(fromidINS) ) |> 
  left_join(communes, by=c("fromidINS")) |> 
  filter(!is.na(travel_time)) |> 
  group_by(fromidINS) |>
  summarise(
    emp1= sum(emp*as.numeric(travel_time<1), na.rm=TRUE),
    emp2= sum(emp*as.numeric(travel_time<2), na.rm=TRUE),
    emp3= sum(emp*as.numeric(travel_time<3), na.rm=TRUE),
    emp4= sum(emp*as.numeric(travel_time<4), na.rm=TRUE),
    emp5= sum(emp*as.numeric(travel_time<5), na.rm=TRUE),
    emp6= sum(emp*as.numeric(travel_time<6), na.rm=TRUE),
    emp7= sum(emp*as.numeric(travel_time<7), na.rm=TRUE),
    emp8= sum(emp*as.numeric(travel_time<8), na.rm=TRUE),
    emp9= sum(emp*as.numeric(travel_time<9), na.rm=TRUE),
    emp10= sum(emp*as.numeric(travel_time<10), na.rm=TRUE),
    emp11= sum(emp*as.numeric(travel_time<11), na.rm=TRUE),
    emp12= sum(emp*as.numeric(travel_time<12), na.rm=TRUE),
    emp13= sum(emp*as.numeric(travel_time<13), na.rm=TRUE),
    emp14= sum(emp*as.numeric(travel_time<14), na.rm=TRUE),
    emp15= sum(emp*as.numeric(travel_time<15), na.rm=TRUE),
    emp16= sum(emp*as.numeric(travel_time<16), na.rm=TRUE),
    emp17= sum(emp*as.numeric(travel_time<17), na.rm=TRUE),
    emp18= sum(emp*as.numeric(travel_time<18), na.rm=TRUE),
    emp19= sum(emp*as.numeric(travel_time<19), na.rm=TRUE),
    emp20= sum(emp*as.numeric(travel_time<20), na.rm=TRUE),
    emp21= sum(emp*as.numeric(travel_time<21), na.rm=TRUE),
    emp22= sum(emp*as.numeric(travel_time<22), na.rm=TRUE),
    emp23= sum(emp*as.numeric(travel_time<23), na.rm=TRUE),
    emp24= sum(emp*as.numeric(travel_time<24), na.rm=TRUE),
    emp25= sum(emp*as.numeric(travel_time<25), na.rm=TRUE),
    emp26= sum(emp*as.numeric(travel_time<26), na.rm=TRUE),
    emp27= sum(emp*as.numeric(travel_time<27), na.rm=TRUE),
    emp28= sum(emp*as.numeric(travel_time<28), na.rm=TRUE),
    emp29= sum(emp*as.numeric(travel_time<29), na.rm=TRUE),
    emp30= sum(emp*as.numeric(travel_time<30), na.rm=TRUE),
    emp31= sum(emp*as.numeric(travel_time<31), na.rm=TRUE),
    emp32= sum(emp*as.numeric(travel_time<32), na.rm=TRUE),
    emp33= sum(emp*as.numeric(travel_time<33), na.rm=TRUE),
    emp34= sum(emp*as.numeric(travel_time<34), na.rm=TRUE),
    emp35= sum(emp*as.numeric(travel_time<35), na.rm=TRUE),
    emp36= sum(emp*as.numeric(travel_time<36), na.rm=TRUE),
    emp37= sum(emp*as.numeric(travel_time<37), na.rm=TRUE),
    emp38= sum(emp*as.numeric(travel_time<38), na.rm=TRUE),
    emp39= sum(emp*as.numeric(travel_time<39), na.rm=TRUE),
    emp40= sum(emp*as.numeric(travel_time<40), na.rm=TRUE),
    emp41= sum(emp*as.numeric(travel_time<41), na.rm=TRUE),
    emp42= sum(emp*as.numeric(travel_time<42), na.rm=TRUE),
    emp43= sum(emp*as.numeric(travel_time<43), na.rm=TRUE),
    emp44= sum(emp*as.numeric(travel_time<44), na.rm=TRUE),
    emp45= sum(emp*as.numeric(travel_time<45), na.rm=TRUE),
    emp46= sum(emp*as.numeric(travel_time<46), na.rm=TRUE),
    emp47= sum(emp*as.numeric(travel_time<47), na.rm=TRUE),
    emp48= sum(emp*as.numeric(travel_time<48), na.rm=TRUE),
    emp49= sum(emp*as.numeric(travel_time<49), na.rm=TRUE),
    emp50= sum(emp*as.numeric(travel_time<50), na.rm=TRUE),
    emp51= sum(emp*as.numeric(travel_time<51), na.rm=TRUE),
    emp52= sum(emp*as.numeric(travel_time<52), na.rm=TRUE),
    emp53= sum(emp*as.numeric(travel_time<53), na.rm=TRUE),
    emp54= sum(emp*as.numeric(travel_time<54), na.rm=TRUE),
    emp55= sum(emp*as.numeric(travel_time<55), na.rm=TRUE),
    emp56= sum(emp*as.numeric(travel_time<56), na.rm=TRUE),
    emp57= sum(emp*as.numeric(travel_time<57), na.rm=TRUE),
    emp58= sum(emp*as.numeric(travel_time<58), na.rm=TRUE),
    emp59= sum(emp*as.numeric(travel_time<59), na.rm=TRUE),
    emp60= sum(emp*as.numeric(travel_time<60), na.rm=TRUE),
    emp61= sum(emp*as.numeric(travel_time<61), na.rm=TRUE),
    emp62= sum(emp*as.numeric(travel_time<62), na.rm=TRUE),
    emp63= sum(emp*as.numeric(travel_time<63), na.rm=TRUE),
    emp64= sum(emp*as.numeric(travel_time<64), na.rm=TRUE),
    emp65= sum(emp*as.numeric(travel_time<65), na.rm=TRUE),
    emp66= sum(emp*as.numeric(travel_time<66), na.rm=TRUE),
    emp67= sum(emp*as.numeric(travel_time<67), na.rm=TRUE),
    emp68= sum(emp*as.numeric(travel_time<68), na.rm=TRUE),
    emp69= sum(emp*as.numeric(travel_time<69), na.rm=TRUE),
    emp70= sum(emp*as.numeric(travel_time<70), na.rm=TRUE),
    emp71= sum(emp*as.numeric(travel_time<71), na.rm=TRUE),
    emp72= sum(emp*as.numeric(travel_time<72), na.rm=TRUE),
    emp73= sum(emp*as.numeric(travel_time<73), na.rm=TRUE),
    emp74= sum(emp*as.numeric(travel_time<74), na.rm=TRUE),
    emp75= sum(emp*as.numeric(travel_time<75), na.rm=TRUE),
    emp76= sum(emp*as.numeric(travel_time<76), na.rm=TRUE),
    emp77= sum(emp*as.numeric(travel_time<77), na.rm=TRUE),
    emp78= sum(emp*as.numeric(travel_time<78), na.rm=TRUE),
    emp79= sum(emp*as.numeric(travel_time<79), na.rm=TRUE),
    emp80= sum(emp*as.numeric(travel_time<80), na.rm=TRUE),
    emp81= sum(emp*as.numeric(travel_time<81), na.rm=TRUE),
    emp82= sum(emp*as.numeric(travel_time<82), na.rm=TRUE),
    emp83= sum(emp*as.numeric(travel_time<83), na.rm=TRUE),
    emp84= sum(emp*as.numeric(travel_time<84), na.rm=TRUE),
    emp85= sum(emp*as.numeric(travel_time<85), na.rm=TRUE),
    emp86= sum(emp*as.numeric(travel_time<86), na.rm=TRUE),
    emp87= sum(emp*as.numeric(travel_time<87), na.rm=TRUE),
    emp88= sum(emp*as.numeric(travel_time<88), na.rm=TRUE),
    emp89= sum(emp*as.numeric(travel_time<89), na.rm=TRUE),
    emp90= sum(emp*as.numeric(travel_time<90), na.rm=TRUE),
    emp91= sum(emp*as.numeric(travel_time<91), na.rm=TRUE),
    emp92= sum(emp*as.numeric(travel_time<92), na.rm=TRUE),
    emp93= sum(emp*as.numeric(travel_time<93), na.rm=TRUE),
    emp94= sum(emp*as.numeric(travel_time<94), na.rm=TRUE),
    emp95= sum(emp*as.numeric(travel_time<95), na.rm=TRUE),
    emp96= sum(emp*as.numeric(travel_time<96), na.rm=TRUE),
    emp97= sum(emp*as.numeric(travel_time<97), na.rm=TRUE),
    emp98= sum(emp*as.numeric(travel_time<98), na.rm=TRUE),
    emp99= sum(emp*as.numeric(travel_time<99), na.rm=TRUE),
    emp100= sum(emp*as.numeric(travel_time<100), na.rm=TRUE),
    emp101= sum(emp*as.numeric(travel_time<101), na.rm=TRUE),
    emp102= sum(emp*as.numeric(travel_time<102), na.rm=TRUE),
    emp103= sum(emp*as.numeric(travel_time<103), na.rm=TRUE),
    emp104= sum(emp*as.numeric(travel_time<104), na.rm=TRUE),
    emp105= sum(emp*as.numeric(travel_time<105), na.rm=TRUE),
    emp106= sum(emp*as.numeric(travel_time<106), na.rm=TRUE),
    emp107= sum(emp*as.numeric(travel_time<107), na.rm=TRUE),
    emp108= sum(emp*as.numeric(travel_time<108), na.rm=TRUE),
    emp109= sum(emp*as.numeric(travel_time<109), na.rm=TRUE),
    emp110= sum(emp*as.numeric(travel_time<110), na.rm=TRUE),
    emp111= sum(emp*as.numeric(travel_time<111), na.rm=TRUE),
    emp112= sum(emp*as.numeric(travel_time<112), na.rm=TRUE),
    emp113= sum(emp*as.numeric(travel_time<113), na.rm=TRUE),
    emp114= sum(emp*as.numeric(travel_time<114), na.rm=TRUE),
    emp115= sum(emp*as.numeric(travel_time<115), na.rm=TRUE),
    emp116= sum(emp*as.numeric(travel_time<116), na.rm=TRUE),
    emp117= sum(emp*as.numeric(travel_time<117), na.rm=TRUE),
    emp118= sum(emp*as.numeric(travel_time<118), na.rm=TRUE),
    emp119= sum(emp*as.numeric(travel_time<119), na.rm=TRUE),
    emp120= sum(emp*as.numeric(travel_time<120), na.rm=TRUE)) |> 
  collect()

  

# setDT(data)
# access <- imap(data, ~ {
#   
# res <- data[, 
#           lapply(times, \(.x) sum(emp*(travel_time_transit<=.x), na.rm=TRUE)), 
#           by="fromidINS"]
res <- accessibility::iso2time(dt2r(data), seuils = seuils) 
res <- r2dt(res)
# res[, `:=`(x=NULL, y=NULL, mode=.y)]
# res
# }) |> bind_rows()


access <- res |>
  as_tibble() |> 
  rename(fromidINS = idINS200) |> 
  left_join(c200ze |> select(com22, fromidINS = idINS, ind), by="fromidINS") |> 
  pivot_longer(cols = starts_with("to"), names_to = "emp", values_to = "temps") |> 
  mutate(emp = as.numeric(str_remove(str_remove(emp, "to"), "k"))*1000)

fromCom <- access |> distinct(com22) |> pull()

mode_l <- access |> 
  group_by(com22, emp) |> 
  filter(!is.na(temps)) |> 
  summarize(temps = weighted_median(temps, w = ind),
            repr = n()) |> 
  filter(repr>15)

tout <- access |> 
  group_by(emp) |> 
  filter(!is.na(temps)) |> 
  summarize(temps = weighted_median(temps, w = ind),
            repr = n()) |> 
  mutate(com22="99999") |> 
  filter(repr>15)

mode_l <- bind_rows(mode_l, tout) |> ungroup() |> 
  mutate(label = case_when(
    com22 == 13203 ~ 'Marseille 3ème arrondissement',
    com22 == 13207 ~ 'Marseille 7ème arrondissement',
    com22 == 13039  ~ 'Fos-sur-Mer',
    com22 == 13001 ~ 'Aix-en-Provence',
    com22 == 99999 ~ 'Tout le territoire',
    TRUE ~ str_c("n", com22)),
    label = factor(
      label, 
      levels = c("Tout le territoire", "Marseille 3ème arrondissement", "Marseille 7ème arrondissement",
                 "Fos-sur-Mer", "Aix-en-Provence")))

access_par_com_car <- ggplot(mode_l) +
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
        panel.grid.major.x = element_line(color="gray80", linewidth = 0.1))





plot(access_par_com_car)
