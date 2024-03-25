library(tidyverse, quietly = TRUE, warn.conflicts = FALSE)
library(glue, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(sf)
source("secrets/azure.R")

conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

arrow::set_cpu_count(8)

# ---- Definition des zones ----
load("baselayer.rda")

cli::cli_alert_info("Masses")
data.table::setDTthreads(4)
arrow::set_cpu_count(4)

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com) |> as.integer()
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com) |> as.integer()

# il y a un potentiellement un petit problème
# normalement, cela devrait être dans c200ze que l'on récupère les indices
# mais les distances en comprennent moins
# du coup on prend ce qu'on a sur les distance
tt <- qs::qread(time_matrix)
froms <- rownames(tt)
tos <- colnames(tt)

actifs <- c200ze |> filter(scot) |> pull(act_mobpro, name = idINS) 
actifs <- actifs[froms]
fuites <- c200ze |> filter(scot) |> pull(fuite_mobpro, name = idINS)
fuites <- fuites[froms]
emplois <- c200ze |> pull(emp_resident, name = idINS) 
emplois <- emplois[tos]

AMP_masses <- list(actifs = actifs, fuites = fuites, emplois = emplois)
bd_write(AMP_masses)

