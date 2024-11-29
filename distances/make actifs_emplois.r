library(tidyverse, quietly = TRUE, warn.conflicts = FALSE)
library(glue, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(sf)

conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# ---- Definition des zones ----
source("mglobals.r")

cli::cli_alert_info("Masses")

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com) |> as.integer()
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com) |> as.integer()

actifs <- c200ze |> filter(scot) |> pull(act_mobpro, name = idINS) 
fuites <- c200ze |> filter(scot) |> pull(fuite_mobpro, name = idINS)
fuites[is.na(fuites)] <- 0
fuites[fuites<0.0001] <- 0.0001
emplois <- c200ze |> pull(emp_resident, name = idINS) 
# 
# actifs_mv <- c200ze |> filter(scot) |> pull(act_mv, name = idINS) 
# emplois_mv <- c200ze |> pull(emp_mvr, name = idINS) 

# fuites_mv <- fuites * (sum(actifs_mv) - sum(emplois_mv))/sum(fuites)
fuites <- fuites * (sum(actifs) - sum(emplois))/sum(fuites)

masses <- list(actifs = actifs, 
               fuites = fuites, 
               emplois = emplois)
# masses_mv <- list(actifs = actifs_mv,
#                   fuites = fuites_mv,
#                   emplois = emplois_mv)
bd_write(masses)
# bd_write(masses_mv)
