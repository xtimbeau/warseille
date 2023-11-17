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

modes <- set_names(c("walk_tblr", 'bike_tblr', 'car_dgr2', 'transit5'))

cli::cli_alert_info("lecture de {.path {idINS_emp_file}}")

idINS_emp_file <- '/space_mounts/data/marseille/distances/src/idINS.parquet' |> glue()

idINS <- read_parquet(idINS_emp_file) |> 
  select(id, fromidINS, toidINS, euc) |> 
  collect() |> 
  as.data.frame() |> 
  setDT()

cli::cli_alert_info(
  "lecture des fichiers de distance
  ....{str_c(files, collapse = ', ')}
  ....dans {.path {r5files_rep}}")

communes <- com2021epci |> pull(INSEE_COM)

distances <- imap(communes, ~{
  data <- map(modes, ~ open_dataset('/space_mounts/data/marseille/distances/src/{.y}') |> distinct(.x) |> 
                select(fromId, toId, travel_time, COMMUNE, DCLT, distance, access_time, egress_time, n_rides) |>
                # rename(travel_time_transit = travel_time) |>
                rename(fromidINS=fromId) |>
                rename(toidINS=toId) |>
                collect() |>
                mutate(COMMUNE = as.character(COMMUNE)))
  transit <- arrow::open_dataset("/space_mounts/data/marseille/distances/src/{.y}" |> distinct(.x) |> glue()) |>
    select(fromidINS, toidINS, travel_time, COMMUNE, DCLT, distance, access_time, egress_time, n_rides) |>
    # rename(travel_time_transit = travel_time) |>
    # filter(distance <= 25000 ) |>
    collect() |>
    mutate(COMMUNE = as.character(COMMUNE))
  data <- append(data, list(transit), 0)
  data <- map(data, ~{
    cols <- intersect(
      names(.y),
      c("fromidINS", "toidINS", "travel_time", "distance", "access_time", "egress_time", "n_rides"))
    dd <- na.omit(.y[, ..cols], "travel_time")
    dd <- merge(dd, idINS[, .(id, fromidINS, toidINS)], by=c("fromidINS", "toidINS"), all.x = TRUE)
    dd <- dd[, `:=`(fromidINS=NULL, toidINS=NULL)]
    setkey(dd, id)
    dd
  })
  names(data) <- modes
  
  
  all_mode <- reduce(modes, function(acc,x) {
    merge(acc, data[[x]], suffixes = c("", str_c("_",x)), by="id", all=TRUE)},
    .init = data.table(id=integer(0), travel_time=integer(0), distance=integer(0))) 
  
  all_mode[, `:=`(distance=NULL,
                  travel_time=NULL)]
  
  
  
  all_mode <- all_mode[travel_time_car <= 90, ]
  all_mode[n_rides==0, access_time := travel_time_transit]
  setkey(all_mode, id)
  
  
  all_mode <- merge(all_mode, idINS[, .(id, euc)], by="id", all.x=TRUE, all.y=FALSE)
  w200 <- (200*sqrt(2)/2/(5*1000/60)) |> as.integer()
  all_mode[
    euc==0,
    `:=`(
      travel_time_walk = w200,
      distance_car = (200*sqrt(2)/2) |> as.integer(),
      travel_time_car = w200,
      travel_time_bike = w200,
      travel_time_transit = w200,
      walktime = w200
    )]
  
  names(data) <- modes
  
  extrapol <- map(
    c("travel_time_bike", "travel_time_walk"),
    ~{
      gc()
      form <- as.formula("log({.y})~log(euc)" |> glue())
      data <- all_mode[euc>=200&is.finite(all_mode[[.y]])&all_mode[[.y]]>0,]
      data <- data[sample.int(n=nrow(data), size=min(nrow(data), 1e+6)), ]
      model <- lm(form, data=data)
      pred <- as.integer(exp(predict(model, all_mode)))
      xx <- all_mode[[.y]]
      grand_ecart <- all_mode[["euc"]]!=0 & abs(log(xx)-log(pred))>1
      grand_ecart[is.na(grand_ecart)] <- FALSE
      garde <- !is.na(xx) & !grand_ecart
      pred[garde] <- xx[garde]
      dd <- data.table(pred, garde)
      names(dd) <- c(.y, str_c("o_", .y))
      dd
    })
  
  extra <- cbind(all_mode[, .(id, euc, distance_car,
                              travel_time_car, o_travel_time_car = TRUE, 
                              travel_time_transit, 
                              time2stop  = access_time , 
                              walktime  = access_time+egress_time)],
                 extrapol[[1]],
                 extrapol[[2]])  


  extra[!is.na(travel_time_transit), o_travel_time_transit := TRUE]
  extra[is.na(travel_time_transit), `:=`(
    travel_time_transit= 9999,
    o_travel_time_transit = FALSE,
    walktime = 9999,
    time2stop = 9999)]
  extra[euc==0, walktime:=w200]
  distances_commune <- extra[, .(id, 
              distance_car,
              travel_time_car, 
              travel_time_bike, 
              travel_time_walk,
              travel_time_transit, 
              time2stop, 
              walktime,
              o_travel_time_car,
              o_travel_time_bike,
              o_travel_time_walk,
              o_travel_time_transit)]
  
  
  
  distances_commune
})

arrow::write_dataset(
  dataset = distances, 
  path = path,
  partitioning = "COMMUNE")



rm(list=ls())
gc(reset=TRUE)