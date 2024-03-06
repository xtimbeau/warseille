# init ---------------
setwd("~/marseille")
rm(list=ls(all.names = TRUE))
gc(reset=TRUE)
library(tidyverse, quietly = TRUE, warn.conflicts = FALSE)
library(accesstars, quietly = TRUE, warn.conflicts = FALSE)
library(tmap, quietly = TRUE, warn.conflicts = FALSE)
library(accessibility, quietly = TRUE, warn.conflicts = FALSE)
library(r3035, quietly = TRUE, warn.conflicts = FALSE)
library(glue, quietly = TRUE, warn.conflicts = FALSE)
library(stars, quietly = TRUE, warn.conflicts = FALSE)
library(data.table, quietly = TRUE, warn.conflicts = FALSE)
library(conflicted, quietly = TRUE, warn.conflicts = FALSE)
library(arrow, quietly = TRUE, warn.conflicts = FALSE)
library(tictoc, quietly = TRUE, warn.conflicts = FALSE)
library(furrr)
tic()
conflicted::conflict_prefer("filter", "dplyr", quiet=TRUE)
conflicted::conflict_prefer("select", "dplyr", quiet=TRUE)
conflicted::conflict_prefer("first", "dplyr", quiet=TRUE)

progressr::handlers(global = TRUE)
progressr::handlers(progressr::handler_progress(format = ":bar :percent :eta", width = 80))
arrow::set_cpu_count(8)

## globals --------------------
cli::cli_alert_info("lecture de baselayer dans {.path {getwd()}}")
load("baselayer.rda")

modes <- set_names(c("walk_tblr", 'bike_tblr', 'car_dgr'))

cli::cli_alert_info("lecture de {.path {idINS_emp_file}}")

# idINS_emp_file <- '/space_mounts/data/marseille/distances/src/idINS.parquet' |> glue()
# 
# idINS <- read_parquet(idINS_emp_file) |> 
#   select(id, fromidINS, toidINS, euc) |> 
#   collect() |> 
#   as.data.frame() |> 
#   setDT()

communes <- com2021epci |> pull(INSEE_COM)

unlink(dist_dts)
dir.create(dist_dts)
plan("multisession", workers = 4)
distances <- future_walk(communes, \(commune) {
  data <- map_dfr(modes, \(mode) {
     arrow::open_dataset("/space_mounts/data/marseille/distances/src/{mode}" |> glue()) |>
                select(fromId, toId, travel_time, COMMUNE, DCLT, distance) |>
                # rename(travel_time_transit = travel_time) |>
                rename(fromidINS=fromId, toidINS=toId) |>
                filter(as.character(COMMUNE)==commune) |>
                collect() |>
                mutate(COMMUNE = as.character(COMMUNE), mode = mode)
    } )
  transit <- arrow::open_dataset("/space_mounts/data/marseille/distances/src/transit5" |> glue()) |>
    select(fromidINS, toidINS, travel_time, COMMUNE, DCLT, access_time, n_rides) |>
    # rename(travel_time = travel_time_transit) |>
    # filter(distance <= 25000 ) |>
    filter(as.character(COMMUNE)==commune) |>
    collect() |>
    mutate(COMMUNE = as.character(COMMUNE), mode='transit')
  data <- data |> bind_rows(transit)
  # extrapol <- map(
  #   c("travel_time_bike", "travel_time_walk"),
  #   ~{
  #     gc()
  #     form <- as.formula("log({modes})~log(euc)" |> glue())
  #     data <- all_mode[euc>=200&is.finite(all_mode[[modes]])&all_mode[[modes]]>0,]
  #     data <- data[sample.int(n=nrow(data), size=min(nrow(data), 1e+6)), ]
  #     model <- lm(form, data=data)
  #     pred <- as.integer(exp(predict(model, all_mode)))
  #     xx <- all_mode[[modes]]
  #     grand_ecart <- all_mode[["euc"]]!=0 & abs(log(xx)-log(pred))>1
  #     grand_ecart[is.na(grand_ecart)] <- FALSE
  #     garde <- !is.na(xx) & !grand_ecart
  #     pred[garde] <- xx[garde]
  #     dd <- data.table(pred, garde)
  #     names(dd) <- c(modes, str_c("o_", modes))
  #     dd
  #   })
  # 
  # extra <- cbind(all_mode[, .(id, euc, distance_car,
  #                             travel_time_car, o_travel_time_car = TRUE, 
  #                             travel_time_transit, 
  #                             time2stop  = access_time , 
  #                             walktime  = access_time+egress_time)],
  #                extrapol[[1]],
  #                extrapol[[2]])  


  # extra[!is.na(travel_time_transit), o_travel_time_transit := TRUE]
  # extra[is.na(travel_time_transit), `:=`(
  #   travel_time_transit= 9999,
  #   o_travel_time_transit = FALSE,
  #   walktime = 9999,
  #   time2stop = 9999)]
  # extra[euc==0, walktime:=w200]
  # distances_commune <- extra[, .(id, 
  #             distance_car,
  #             travel_time_car, 
  #             travel_time_bike, 
  #             travel_time_walk,
  #             travel_time_transit, 
  #             time2stop, 
  #             walktime,
  #             o_travel_time_car,
  #             o_travel_time_bike,
  #             o_travel_time_walk,
  #             o_travel_time_transit)]
  # 
  dir.create(str_c(dist_dts, "/", commune))
  fname <- str_c(dist_dts, "/", commune, "/allmode.parquet")
  write_parquet(data, fname)

  return(fname)
  
  }, .progress = TRUE)

rm(list=ls())
gc(reset=TRUE)