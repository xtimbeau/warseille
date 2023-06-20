#pour utiliser DVF

library(tidyverse)
library(sf)
library(nuvolos)

con <- get_connection()
db <- tbl(con,"DV3FV231_MUTATION") 

local <- db |> 
  dplyr::filter(CODDEP=="13") %>% 
  dplyr::select(IDMUTATION, DATEMUT, VALEURFONC) %>%
  collect()
