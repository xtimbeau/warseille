library(sf)
library(tidyverse)
library(glue)
shps <- list.files(pattern = "*.shp", recursive=TRUE, full.names = TRUE) |> 
  keep(~str_detect(.x, "rfp_gr"))
rows <- map_dbl(shps, ~nrow(st_read(.x)))
shp1 |> filter(nlocburx>0)


# SQL
psql <- "/opt/homebrew/opt/postgresql@13/bin/psql -h localhost -p 5432 -U postgres"
system(glue('{psql} -c "CREATE DATABASE rfp2019;"'))
system('{psql} -d rfp2019 -c "CREATE EXTENSION postgis; CREATE EXTENSION postgis_topology;"' %>% glue)
system('{psql} -d rfp2019 -f "rfp_2019_d13.sql"' %>% glue)
