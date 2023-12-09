#pour utiliser DVF

library(tidyverse)
library(sf)
library(nuvolos)

con <- get_connection()
db <- tbl(con,"DV3FV231_MUTATION") 

DV3F13 <- db |> 
  dplyr::filter(CODDEP=="13") %>% 
  dplyr::select(IDMUTATION, DATEMUT, VALEURFONC) %>%
  collect()

DV3F30 <- db |> 
  dplyr::filter(CODDEP=="30") %>% 
  dplyr::select(IDMUTATION, DATEMUT, VALEURFONC) %>%
  collect()

DV3F80 <- db |> 
  dplyr::filter(CODDEP=="80") %>% 
  dplyr::select(IDMUTATION, DATEMUT, VALEURFONC) %>%
  collect()

DV3F84 <- db |> 
  dplyr::filter(CODDEP=="84") %>% 
  dplyr::select(IDMUTATION, DATEMUT, VALEURFONC) %>%
  collect()


