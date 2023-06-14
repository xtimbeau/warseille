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

modes <- names(r5_output)
files <- r5_output

cli::cli_alert_info("lecture de {.path {idINS_emp_file}}")

idINS <- read_parquet(idINS_emp_file) |> 
  select(id, fromidINS, toidINS, euc) |> 
  collect() |> 
  as.data.frame() |> 
  setDT()

cli::cli_alert_info(
  "lecture des fichiers de distance
  ....{str_c(files, collapse = ', ')}
  ....dans {.path {r5files_rep}}")
data <- map(files, ~{
  setDT(read_parquet("{r5files_rep}/{.x}" |> glue()) |> as.data.frame())
})

data <- map(data, ~{
  cols <- intersect(
    names(.x),
    c("fromidINS", "toidINS", "travel_time", "distance", "access_time", "egress_time", "n_rides"))
  dd <- na.omit(.x[, ..cols], "travel_time")
  dd <- merge(dd, idINS[, .(id, fromidINS, toidINS)], by=c("fromidINS", "toidINS"), all.x = TRUE)
  dd <- dd[, `:=`(fromidINS=NULL, toidINS=NULL)]
  setkey(dd, id)
  dd
})
names(data) <- modes


# on combine les tables de distances correspondant à chaque mode de transport en un seul dataframe
all_mode <- reduce(modes, function(acc,x) {
  merge(acc, data[[x]], suffixes = c("", str_c("_",x)), by="id", all=TRUE)},
  .init = data.table(id=integer(0), travel_time=integer(0), distance=integer(0))) 

all_mode[, `:=`(distance=NULL,
                travel_time=NULL)]



all_mode <- all_mode[travel_time_car <= 90, ]
all_mode[n_rides==0, access_time := travel_time_transit]
setkey(all_mode, id)

# on ne prolonge plus aussi loin, ça ne sert à rien 
# all_mode.x <- merge(idINS, all_mode, by="id", all.x=TRUE, all.y=FALSE)
# on a ajouté des carreaux, donc des euc==0
# 
# On ajoute euc à all_mode
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

rm(idINS)
gc()

# on considère que la voiture a été générée suffisamment loin (120 minutes)
# seuls le vélo et la marche sont extrapolés
cli::cli_alert_info("extrapolation (vélo+marche)")
extrapol <- map(
  c("travel_time_bike", "travel_time_walk"),
  ~{
    gc()
    form <- as.formula("log({.x})~log(euc)" |> glue())
    data <- all_mode[euc>=200&is.finite(all_mode[[.x]])&all_mode[[.x]]>0,]
    data <- data[sample.int(n=nrow(data), size=min(nrow(data), 1e+6)), ]
    model <- lm(form, data=data)
    pred <- as.integer(exp(predict(model, all_mode)))
    xx <- all_mode[[.x]]
    grand_ecart <- all_mode[["euc"]]!=0 & abs(log(xx)-log(pred))>1
    grand_ecart[is.na(grand_ecart)] <- FALSE
    garde <- !is.na(xx) & !grand_ecart
    pred[garde] <- xx[garde]
    dd <- data.table(pred, garde)
    names(dd) <- c(.x, str_c("o_", .x))
    dd
  })

# do.call se prend les pieds dans le tapis et fait un merge data.frame ?
# comme ça ca va bcp plus vite -- ce qui est normal
extra <- cbind(all_mode[, .(id, euc, distance_car,
                            travel_time_car, o_travel_time_car = TRUE, 
                            travel_time_transit, 
                            time2stop  = access_time , 
                            walktime  = access_time+egress_time)],
               extrapol[[1]],
               extrapol[[2]])  
# on combine les extrapolations dans une nouvelle matrice 

# on ajoute le temps aux arrêts
cli::cli_alert_info("complétion de transit")

extra[!is.na(travel_time_transit), o_travel_time_transit := TRUE]
extra[is.na(travel_time_transit), `:=`(
  travel_time_transit= 9999,
  o_travel_time_transit = FALSE,
  walktime = 9999,
  time2stop = 9999)]
extra[euc==0, walktime:=w200]
cli::cli_alert_info("écriture de {.path {distances_file}}")
arrow::write_parquet(
  extra[, .(id, 
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
            o_travel_time_transit)], 
  sink = distances_file)
t_toc <- toc(TRUE, TRUE)
timer <- tolower(lubridate::seconds_to_period(round(t_toc$toc-t_toc$tic)))

if(nrow(extra[is.na(distance_car),])>0) { 
  cli::cli_alert_danger(
    "{nrow(extra)} paires d'o/d de distances avec {nrow(extra[is.na(proba_car),]} nas, en {timer}")
} else {
  cli::cli_alert_success("{nrow(extra)} paires d'o/d de distances pas de na, en {timer}")}

rm(list=ls())
gc(reset=TRUE)