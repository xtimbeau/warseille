library(tidyverse)
library(ofce)
library(sf)
library(tmap)
library(arrow)
library(r3035)
library(conflicted)
source("mglobals.r")
conflict_prefer( "dt2r", "r3035")
conflict_prefer( "r2dt", "r3035")
conflict_prefer( "idINS2square", "r3035")
conflict_prefer_all("dplyr", quiet = TRUE)

c200ze <- qs::qread(c200ze_file)
times <- seq(1, 120, 1)
times <- set_names(times, str_c("t", times))
seuils <- c(10000, 20000, 50000, 100000, 200000, 300000, 4000000, 500000)

modes <- set_names(c("walk_tblr", "bike_tblr",
                     'transit',"car_dgr"))
temps <- 1:120

txt <- str_c("opp", temps, "= sum(opp*as.numeric(travel_time<=", 60*temps,"), na.rm=TRUE)") 
unlink("/tmp/access.r")
ff <- file("/tmp/access.r", open = "at")
writeLines("dd <- dd |> summarise(", ff)
walk(head(txt, -1), ~writeLines(str_c(.x, ","), ff))
writeLines(str_c(dplyr::last(txt), ")"), ff)
close(ff)

actif <- c200ze |> 
  st_drop_geometry() |> 
  mutate(act_pot = ind_15_64 * tact1564) |> 
  filter(act_pot > 0) |>
  select(fromidINS=idINS, act_pot) |> 
  write_dataset(("/tmp/actif"))

actif <- open_dataset("/tmp/actif") |> 
  to_duckdb()

zae <- st_read("access/zae/perimetre_zae.shp") |> 
  st_transform(3035)

tos <- arrow::open_dataset(dist_dts) |>
  to_duckdb() |> 
  distinct(toidINS) |> 
  collect() |> 
  r3035::sidINS2sf(idINS = "toidINS") |> 
  sf::st_join(zae) |> 
  filter(!is.na(id)) |>
  st_drop_geometry() |> 
  to_duckdb()

access_to_zae <- map_dfr(
  modes, ~{
    ddo <- arrow::open_dataset(dist_dts) |>
      to_duckdb() |> 
      filter(mode == .x) |> 
      left_join(tos, by=join_by(toidINS)) |> 
      filter(!is.na(id))
    
    dd <- actif |> 
      left_join(ddo, by="fromidINS") |>
      filter(!is.na(toidINS) ) |> 
      filter(!is.na(travel_time)) |> 
      rename(opp = act_pot) |>
      group_by(fromidINS, id) |> 
      summarize(opp = first(opp), 
                ttmin = min(travel_time, na.rm=TRUE), 
                ttmed = median(travel_time, na.rm=TRUE),
                ttmax = max(travel_time, na.rm=TRUE),
                .groups = "drop") |> 
      collect() |> 
      group_by(id) |> 
      mutate(travel_time = ttmed)
    
    source("/tmp/access.r", local = TRUE)
    
    dd |> 
      collect() |> 
      mutate(mode = .x) |> 
      relocate(id, mode) |> 
      mutate(across(-c(id, mode), ~replace_na(.x, 0)))
    
  }, .progress=TRUE)

acc2zae <- access_to_zae |> 
  select(id, mode, all_of(str_c("opp", seq(5,60,5)))) |> 
  collect() |> 
  rename_with(~str_replace(.x, "opp", "m")) 

bd_write(acc2zae)
vroom::vroom_write(acc2zae, file = "acc2zae.csv")

tmap_mode("view") 
tm_shape(zae |> left_join(acc2zae, by = join_by(id)) |> filter(m45>0, mode == "car_dgr"))+tm_borders()+tm_fill(fill = "m45", fill.scale = tm_scale_continuous(trans="log"))
acc2zae.sf <- zae |> 
  left_join(acc2zae, by = join_by(id)) |>
  filter(!is.na(mode)) |> 
  mutate(
    m45q = santoku::chop_deciles(m45)  )

names <- acc2zae.sf |>
  st_drop_geometry() |> 
  group_by(m45q) |> 
  summarise(m45 = mean(m45)) |> 
  mutate(m45 = str_c(round(m45/1000), "k")) |> 
  pull(m45, name = m45q ) 
decor_carte <- bd_read("decor_carte")
(acc2zae_plot <- ggplot()+
    decor_carte +
    ofce::theme_ofce_void(axis.text = element_blank()) +
    geom_sf(data=acc2zae.sf , aes(fill=m45q), col=NA)+
    cols4all::scale_fill_discrete_c4a_div("sunset_sunrise_diverging", 
                                          name = "Accessibilité 45m. : ",
                                          labels = names)+
    guides(fill = guide_legend(nrow = 1)) +
    ggspatial::annotation_scale(line_width = 0.2, height = unit(0.1, "cm"), 
                     text_cex = 0.4, pad_y = unit(0.1, "cm"))+
    facet_wrap(vars(mode)))
