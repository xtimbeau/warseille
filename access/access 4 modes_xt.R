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
seuils <- c(1000, 2000, 5000, 10000, 20000, 25000, 50000, 100000, 250000)

modes <- set_names(c("walk_tblr", "bike_tblr", 'transit5',"car_dgr2"))
emploi <- c200ze |> filter(emp>0) |> select(toidINS=idINS, emp) |> 
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
    if(.x == "transit5") 
      dd <- dd |>
        select(fromidINS, toidINS, travel_time, COMMUNE, DCLT)
    else 
      dd <- dd |>
        select(fromidINS=fromId, toidINS=toId, travel_time, COMMUNE, DCLT)
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

t_access <- imap(modes, ~{
  res <- access |> filter(mode== .x) |> select(-mode)
  res <- accessibility::iso2time(dt2r(res), seuils = seuils) 
  res <- r2dt(res)
  res[, `:=`(x=NULL, y=NULL, mode=.x)]
  res
}) |> 
  bind_rows() 

t_access <- t_access |> 
  mutate(geometry=idINS2square(idINS200),
         mode = factor(mode, 
                       c("car_dgr2", "transit5", "bike_tblr", "walk_tblr"), 
                       c("Voiture", "Transport en commun", "Vélo", "Marche à pied"))) |> 
  st_as_sf(crs=3035)

write_csv(t_access |> st_drop_geometry(), file="output/access.csv")
qs::qsave(t_access, "output/acces4modes.sqs")
load(decor_carte_file)
qs::qsave(decor_carte, "output/decor_carte.sqs")

(access_4modes_walk <- ggplot()+
    decor_carte +
    ofce::theme_ofce_void(base_family = "Roboto", axis.text = element_blank()) +
    geom_sf(data=t_access, aes(fill=to100k), col=NA)+
    scico::scale_fill_scico(palette="hawaii", na.value=NA, direction=-1, name = "mn")+
    annotation_scale(line_width = 0.2, height = unit(0.1, "cm"), 
                     text_cex = 0.4, pad_y = unit(0.1, "cm"))+
    facet_wrap(vars(mode)))

ofce::graph2png(access_4modes_bike, rep=output_rep)

# ggplot()+
#   decor_carte +
#   geom_sf(data=diff, aes(fill=to5k), col="white", linewidth=0.01)+
#   geom_sf(data=stops.sf, aes(), col="red", size=0.25, alpha=0.75,show.legend = FALSE) +
#   scico::scale_fill_scico(palette="vik", na.value=NA, midpoint=0)

#on compare bike et bike_tblr


data_ntblr <- c()
bike_ntblr <- read_parquet('/space_mounts/data/marseille/distances/bike.parquet')
data_ntblr <- append(data_ntblr, list(bike_ntblr), 0)
rm(bike_ntblr)

access_ntblr <- imap(data_ntblr, ~{
  ld <- merge(.x, 
              c200ze[emp>0, .(toidINS=idINS, emp, geometry)],
              by=c("toidINS"), all.y=TRUE) %>%
    drop_na(fromidINS) %>% 
    st_as_sf(crs=3035)
  setDT(ld)
  res <- ld[ , lapply(times, \(.x) sum(emp*(travel_time<=.x), na.rm=TRUE)), by="fromidINS"]
  res <- accessibility::iso2time(dt2r(res), seuils = seuils) 
  res <- r2dt(res)
  res[, `:=`(x=NULL, y=NULL, mode=.y)]
  res
}) |> 
  bind_rows() 

access_ntblr <- access_ntblr |> 
  mutate(geometry=idINS2square(idINS200),
         mode = factor(mode, 
                       c("car", "transit", "bike", "walk"), 
                       c("Voiture", "Transport en commun", "Vélo", "Marche à pied"))) |> 
  st_as_sf(crs=3035)


(access_4modes_bike_ntblr <- ggplot()+
    decor_carte +
    ofce::theme_ofce_void(base_family = "Roboto", axis.text = element_blank()) +
    geom_sf(data=access_ntblr, aes(fill=to10k), col=NA)+
    scico::scale_fill_scico(palette="hawaii", na.value=NA, direction=-1, name = "mn")+
    annotation_scale(line_width = 0.2, height = unit(0.1, "cm"), 
                     text_cex = 0.4, pad_y = unit(0.1, "cm"))+
    facet_wrap(vars(mode)))

emploi <- c200ze |> filter(emp>0) |> select(toidINS=idINS, emp) |> 
  to_duckdb()

temps <- 1:120
txt <- str_c("emp", temps, "= sum(emp*as.numeric(travel_time<", temps,"), na.rm=TRUE),")

fileConn<-file("output.txt")
writeLines(txt, fileConn)

#on essaye avec to_duckdb() pour la voiture
data <- arrow::open_dataset("/space_mounts/data/marseille/distances/src/car_dgr2") |>
  to_duckdb() |>
  select(fromId, toId, travel_time, COMMUNE, DCLT) |>
  rename(fromidINS=fromId) |>
  rename(toidINS=toId) |>
  left_join(emploi, by=c("toidINS")) |>
  filter(!is.na(fromidINS) ) |> 
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

acc <- map(set_names(c(1,5,10)), ~ partial(function(tt, i) sum(emp*as.numeric(tt<i)), i =.x))
across()


# setDT(data)
# access <- imap(data, ~ {
# res <- data[ , lapply(times, \(.x) sum(emp*(travel_time_transit<=.x), na.rm=TRUE)), by="fromidINS"]
res <- accessibility::iso2time(dt2r(data), seuils = seuils) 
res <- r2dt(res)
# res[, `:=`(x=NULL, y=NULL, mode=.y)]
# res
# }) |> bind_rows()


access <- res |> 
  mutate(geometry=idINS2square(idINS200)) |> 
  st_as_sf(crs=3035)

(access_4modes_car <- ggplot()+
    decor_carte +
    ofce::theme_ofce_void(base_family = "Roboto", axis.text = element_blank()) +
    geom_sf(data=access, aes(fill=to100k), col=NA)+
    scico::scale_fill_scico(palette="hawaii", na.value=NA, direction=-1, name = "mn")+
    annotation_scale(line_width = 0.2, height = unit(0.1, "cm"), 
                     text_cex = 0.4, pad_y = unit(0.1, "cm")))




