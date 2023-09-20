#' calcul du temps de trajet selon le système de routing choisi.
#' /code{iso_ttm} calcule un temps de trajets
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
iso_ttm <- function(o, d, tmax, routing)
{
  r <- switch(routing$type,
              "r5" = r5_ttm(o, d, tmax, routing),
              "r5_cong" = r5_ttm(o, d, tmax, routing),
              "r5_ext" = r5_ettm(o, d, tmax, routing),
              "r5_cong" = r5_ttm(o, d, tmax, routing),
              "r5_di" = r5_di(o, d, tmax, routing),
              "r5_cong" = r5_ttm(o, d, tmax, routing),
              "otpv1" = otpv1_ttm(o, d, tmax, routing),
              "osrm" = osrm_ttm(o, d, tmax, routing),
              "dt"= dt_ttm(o, d, tmax, routing),
              "euclidean" = euc_ttm(o, d, tmax, routing),
              "dodgr" = dodgr_ttm(o, d, tmax, routing))
  r
}

#' définit le type du routeur
#'
#' @param routing système de routing
safe_ttm <- function(routing)
{
  switch(routing$type,
         "r5" = r5_ttm,
         "otpv1" = otpv1_ttm,
         "osrm" = osrm_ttm,
         "dt"= dt_ttm,
         "euclidean" = euc_ttm,
         "dodgr" = dodgr)
}

#' définit le type du routeur
#'
#' @param delay délai pour l'heure de départ
#' @param routing système de routing
delayRouting <- function(delay, routing)
{
  switch(routing$type,
         "r5" = {
           res <- routing
           res$departure_datetime <-
             as.POSIXct(routing$departure_datetime+delay*60,
                        format = "%d-%m-%Y %H:%M:%S")
           res},
         "otpv1" = {routing}, # to do
         "osrm" = {routing}, # no departure time
         "data.table"= {routing},
         "dodgr"= {routing}) # rien to do
}

#' wrapper pour travel_time_matrix
#'
#' @param ... cf r5r::travel_time_matrix
safe_r5_ttm <- purrr::safely(r5r::travel_time_matrix)

#' fonction de récupération
#'
#' @inheritParams r5r::travel_time_matrix
#'

quiet_r5_ttm <- function(...) {
  utils::capture.output(
    r <- safe_r5_ttm(...),
    file=NULL,
    type=c("output", "message"),
    split = FALSE, append = TRUE)
  r
}

#' calcul du temps de trajet avec r5
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
r5_ttm <- function(o, d, tmax, routing)
{
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  res <- quiet_r5_ttm(
    r5r_core = routing$core,
    origins = o,
    destinations = d,
    mode=routing$mode,
    departure_datetime = routing$departure_datetime,
    max_walk_time = routing$max_walk_time,
    max_trip_duration = tmax+1,
    time_window = as.integer(routing$time_window),
    percentiles = routing$percentile,
    walk_speed = routing$walk_speed,
    bike_speed = routing$bike_speed,
    max_rides = routing$max_rides,
    max_lts = routing$max_lts,
    n_threads = routing$n_threads,
    verbose=FALSE,
    progress=FALSE)
  if(!is.null(res$error))
  {
    gc()
    res <- safe_r5_ttm(
      r5r_core = routing$core,
      origins = o,
      destinations = d,
      mode=routing$mode,
      departure_datetime = routing$departure_datetime,
      max_walk_time = routing$max_walk_time,
      max_trip_duration = tmax+1,
      time_window = as.integer(routing$time_window),
      percentiles = routing$percentile,
      walk_speed = routing$walk_speed,
      bike_speed = routing$bike_speed,
      max_rides = routing$max_rides,
      n_threads = routing$n_threads,
      verbose=FALSE,
      progress=FALSE)
    if(is.null(res$error)) logger::log_warn("second r5::travel_time_matrix ok")
  }
  
  if (is.null(res$error)&&nrow(res$result)>0)
    res$result[, `:=`(fromId=as.integer(from_id), toId=as.integer(to_id), travel_time=as.integer(travel_time_p50))]
  else
  {
    logger::log_warn("error r5::travel_time_matrix, give an empty matrix after 2 attemps")
    res$result <- data.table(fromId=numeric(), toId=numeric(), travel_time=numeric())
  }
  res
}


#' wrapper pour expanded_travel_time_matrix
#'
#' @param ... cf r5r::travel_time_matrix
safe_r5_ettm <- purrr::safely(r5r::expanded_travel_time_matrix)

#' fonction de récupération
#'
#' @inheritParams r5r::expanded_travel_time_matrix
#'

quiet_r5_ettm <- function(...) {
  utils::capture.output(
    r <- safe_r5_ettm(...),
    file=NULL,
    type=c("output", "message"),
    split = FALSE, append = TRUE)
  r
}

#' calcul du temps de trajet avec r5
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
r5_ettm <- function(o, d, tmax, routing)
{
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  res <- quiet_r5_ettm(
    r5r_core = routing$core,
    origins = o,
    destinations = d,
    mode=routing$mode,
    departure_datetime = routing$departure_datetime,
    max_walk_time = routing$max_walk_time,
    max_trip_duration = tmax+1,
    time_window = as.integer(routing$time_window),
    walk_speed = routing$walk_speed,
    bike_speed = routing$bike_speed,
    max_rides = routing$max_rides,
    max_lts = routing$max_lts,
    n_threads = routing$n_threads,
    breakdown = TRUE,
    verbose=FALSE,
    progress=FALSE)
  if(!is.null(res$error))
  {
    gc()
    res <- safe_r5_ettm(
      r5r_core = routing$core,
      origins = o,
      destinations = d,
      mode=routing$mode,
      departure_datetime = routing$departure_datetime,
      max_walk_time = routing$max_walk_time,
      max_trip_duration = tmax+1,
      time_window = as.integer(routing$time_window),
      percentiles = routing$percentile,
      walk_speed = routing$walk_speed,
      bike_speed = routing$bike_speed,
      max_rides = routing$max_rides,
      n_threads = routing$n_threads,
      breakdown = TRUE,
      verbose=FALSE,
      progress=FALSE)
    if(is.null(res$error)) logger::log_warn("second r5r::expanded_travel_time_matrix ok")
  }
  
  if (is.null(res$error)&&nrow(res$result)>0){
    res$result[, `:=`(fromId=as.integer(from_id), toId=as.integer(to_id), travel_time=as.integer(total_time),
                      n_rides = as.integer(n_rides), access_time = as.integer(access_time), wait_time = as.integer(wait_time),
                      ride_time = as.integer(ride_time), transfer_time = as.integer(transfer_time))]
  }
  else
  {
    logger::log_warn("error r5r::expanded_travel_time_matrix, give an empty matrix after 2 attemps")
    res$result <- data.table(fromId=numeric(), toId=numeric(), travel_time=numeric())
  }
  res
}

#' wrapper pour detailed_itineraries
#'
#' @inheritParams r5r::detailed_itineraries
safe_r5_di <- purrr::safely(r5r::detailed_itineraries)

#' calcul des itinéraires détaillés avec r5r
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
r5_di <- function(o, d, tmax, routing) {
  gc()
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  od <- ksplit(
    CJ(o = o$id, d=d$id),
    k=max(1,ceiling(nrow(o)*nrow(d)/routing$max_rows)))
  tt <- Sys.time()
  res <- map(od, function(od_element) {
    oCJ <- data.table(id=od_element$o)
    dCJ <- data.table(id=od_element$d)
    res <- safe_r5_di(
      all_to_all = TRUE,
      r5r_core = routing$core,
      origins = o[oCJ, on="id"],
      destinations = d[dCJ, on="id"],
      mode=routing$mode,
      mode_egress="WALK",
      departure_datetime = routing$departure_datetime,
      max_walk_time = routing$max_walk_time,
      max_bike_time = Inf,
      max_trip_duration = tmax+1,
      walk_speed = routing$walk_speed,
      bike_speed = routing$bike_speed,
      max_rides = routing$max_rides,
      max_lts = routing$max_lts,
      shortest_path= TRUE,
      n_threads = routing$n_threads,
      verbose=FALSE,
      progress=FALSE,
      drop_geometry=is.null(routing$elevation_tif))
  })
  res <- purrr::transpose(res)
  res$result <- rbindlist(res$result)
  if("geometry"%in%names(res$result))
    res$result <- st_as_sf(res$result)
  res$error <- compact(res$error)
  if(length(res$error)==0) res$error <- NULL
  logger::log_debug("calcul de distances ({round(as.numeric(Sys.time()-tt), 2)} s. {nrow(od)} paires)")
  
  if(is.null(res$error)) {
    if(nrow(res$result)>0) {
      if(!is.null(routing$elevation_tif)) {
        # on discretise par pas de 10m pour le calcul des dénivelés
        # ca va plus vite que la version LINESTRING (x10)
        # avec un zoom à 13 les carreaux font 5x5m
        # mais on n'attrape pas le pont de l'ile de Ré
        tt <- Sys.time()
        pp <- sf::st_coordinates(
          sf::st_cast(
            sf::st_segmentize(
              st_geometry(res$result),
              dfMaxLength = routing$dfMaxLength),
            "MULTIPOINT"))
        elvts <- data.table(id = pp[,3], h = terra::extract(routing$elevation_data, pp[, 1:2]))
        setnames(elvts, "h.elevation", "h")
        elvts[, h:= nafill(h, type="locf")]
        elvts[, dh:= h-data.table::shift(h, type="lag", fill=NA), by="id"]
        deniv <- elvts[, .(deniv=sum(dh, na.rm=TRUE), deniv_pos=sum(dh[dh>0], na.rm=TRUE)), by="id"]
        deniv[, id:=NULL]
        logger::log_debug("calcul d'élévation ({round(as.numeric(Sys.time()-tt), 2)} s.)")
        resdi <- cbind(as.data.table(st_drop_geometry(res$result)), deniv)
      } else {
        resdi <- as.data.table(res$result)
        resdi[, `:=`(deniv=NA, deniv_pos=NA)]
      }
      setnames(resdi, c("to_id", "from_id"), c("toId", "fromId"))
      resdi <- resdi[ , .(travel_time = as.integer(sum(total_duration)),
                          distance = sum(distance),
                          deniv = sum(deniv),
                          deniv_pos = sum(deniv_pos),
                          legs = .N), by=c("fromId", "toId")]
      resdi[, `:=`(fromId=as.integer(fromId), toId=as.integer(toId))]
      
      res$result <- resdi
    }
  } else # quand il y a erreur on renvoie une table nulle
    res$result <- data.table(
      fromId=numeric(),
      toId=numeric(),
      travel_time=numeric(),
      distance=numeric(),
      deniv = numeric(),
      deniv_pos = numeric(),
      legs=numeric())
  res
}

#' calcul du temps de trajet avec une distance euclidienne
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
#' @import sf
euc_ttm <- function(o, d, tmax, routing)
{
  mode <- routing$mode
  vitesse <- routing$speed
  
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  
  o_3035 <- sf_project(from=st_crs(4326), to=st_crs(3035), o[, .(lon, lat)])
  d_3035 <- sf_project(from=st_crs(4326), to=st_crs(3035), d[, .(lon, lat)])
  dist <- rdist::cdist(X=o_3035, Y=d_3035, metric="euclidean", p=2)
  colnames(dist) <- d$id
  rownames(dist) <- o$id
  dt <- data.table(dist, keep.rownames=TRUE)
  setnames(dt, "rn", "fromId")
  dt[, fromId:=as.integer(fromId)]
  dt <- melt(dt, id.vars="fromId", variable.name="toId", value.name = "distance", variable.factor = FALSE)
  dt[, travel_time := distance/1000/vitesse*60]
  dt <- dt[travel_time<tmax,]
  dt[, `:=`(toId = as.integer(toId), travel_time = as.integer(ceiling(travel_time)))]
  list(error=NULL,
       result=dt)
}

#' calcul du temps de trajet avec otp. Ne marche pas.
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
otpv1_ttm <- function(o, d, tmax, routing)
{
  # ca marche pas parce que OTP ne renvoie pas de table
  # du coup il faudrait faire ça avec les isochrones
  # ou interroger OTP paire par paire
  # la solution ici est très très lente et donc pas utilisable
  
  o[, `:=`(k=1, fromId=id, fromlon=lon, fromlat=lat)]
  d[, `:=`(k=1, toId=id, tolon=lon, tolat=lat)]
  paires <- merge(o,d, by="k", allow.cartesian=TRUE)
  temps <- furrr::future_map_dbl(1:nrow(paires), ~{
    x <- paires[.x, ]
    t <- otpr::otp_get_times(
      routing$otpcon,
      fromPlace= c(x$fromlat, x$fromlon),
      toPlace= c(x$tolat, x$tolon),
      mode= routing$mode,
      date= routing$date,
      time= routing$time,
      maxWalkDistance= routing$maxWalkDistance,
      walkReluctance = routing$walkReluctance,
      arriveBy = routing$arriveBy,
      transferPenalty = routing$transferPenalty,
      minTransferTime = routing$minTransferTime,
      detail = FALSE,
      includeLegs = FALSE)
    if(t$errorId=="OK") t[["duration"]]
    else NA
  })
  paires[ , .(fromId, toId)] [, temps:=as.integer(temps)]
}

#' calcul du temps de trajet avec osrm
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
osrm_ttm <- function(o, d, tmax, routing)
{
  options(osrm.server = routing$osrm.server,
          osrm.profile = routing$osrm.profile)
  safe_table <- purrr::safely(osrm::osrmTable)
  l_o <- o[, .(id, lon, lat)]
  l_d <- d[, .(id, lon, lat)]
  tt <- safe_table(
    src = l_o,
    dst= l_d,
    exclude=NULL,
    gepaf=FALSE,
    measure="duration")
  if(!is.null(tt$error))
  {
    gc()
    logger::log_warn("deuxieme essai osrm")
    tt <- safe_table(
      src = l_o,
      dst= l_d,
      exclude=NULL,
      gepaf=FALSE,
      measure="duration")
  }
  
  if(is.null(tt$error))
  {
    dt <- data.table(tt$result$duration, keep.rownames = TRUE)
    dt[, fromId:=rn |> as.integer()] [, rn:=NULL]
    dt <- melt(dt, id.vars="fromId", variable.name="toId", value.name = "travel_time", variable.factor = FALSE)
    dt <- dt[travel_time < tmax,]
    dt[, `:=`(toId = as.integer(toId), travel_time = as.integer(ceiling(travel_time)))]
    tt$result <- dt
  }
  else
    logger::log_warn("error osrm::osrmTable, give an empty matrix after 2 attemps")
  tt
}

#' calcul du temps de trajet avec data.table (??)
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
dt_ttm <- function(o, d, tmax, routing)
{
  o_rid <- merge(o[, .(oid=id, x, y)], routing$fromId[, .(rid=id, x, y)], by=c("x", "y"))
  d_rid <- merge(d[, .(did=id, x, y)], routing$toId[, .(rid=id, x, y)], by=c("x", "y"))
  ttm <- routing$time_table[(fromId%in%o_rid$rid), ][(toId%in%d_rid$rid),][(travel_time<tmax), ]
  ttm <- merge(ttm, o_rid[, .(oid, fromId=rid)], by="fromId")
  ttm <- merge(ttm, d_rid[, .(did, toId=rid)], by="toId")
  ttm <- ttm[, `:=`(fromId=NULL, toId=NULL)]
  setnames(ttm,old=c("oid", "did"), new=c("fromId", "toId"))
  list(
    error=NULL,
    result=ttm
  )
}

#' setup du système de routing r5
#'
#' @param data_path path
#' @param verbose par défaut, FALSE
#' @param temp_dir par défaut, FALSE
#' @param use_elevation par défaut, FALSE
#' @param overwrite par défaut, FALSE
#'
#' @export
quick_setup_r5 <- function (data_path, verbose = FALSE, temp_dir = FALSE,
                            use_elevation = FALSE, overwrite = FALSE) {
  
  rlang::check_installed("rJava", reason = "rJava est nécessaire pour r5r")
  rlang::check_installed("r5r", reason = "r5r est nécessaire pour ce calcul")
  
  checkmate::assert_directory_exists(data_path)
  checkmate::assert_logical(verbose)
  checkmate::assert_logical(temp_dir)
  checkmate::assert_logical(use_elevation)
  checkmate::assert_logical(overwrite)
  rJave::.jinit()
  data_path <- path.expand(data_path)
  
  any_network <- length(grep("network.dat", list.files(data_path))) > 0
  if (!(any_network)) stop("\nA network is needed")
  
  jars <- list.files(path=file.path(.libPaths()[1], "r5r", "jar")) |>
    purrr::keep(~ stringr::str_detect(.x,"^r5-[:graph:]*.jar"))
  jars_date <- stringr::str_extract(jars, pattern="[:digit:]{8}") |> as.numeric()
  jar <- jars[which.max(jars_date)]
  jar_file <- file.path(.libPaths()[1], "r5r", "jar", jar)
  r5r_jar <- file.path(.libPaths()[1], "r5r", "jar", "r5r_0_6_0.jar")
  .jaddClassPath(path = r5r_jar)
  .jaddClassPath(path = jar_file)
  dat_file <- file.path(data_path, "network.dat")
  if (checkmate::test_file_exists(dat_file) && !overwrite) {
    r5r_core <- .jnew("org.ipea.r5r.R5RCore", data_path,
                      verbose)
    message("\nUsing cached network.dat from ", dat_file)
  }
  else {
    return(NULL)
  }
  r5r_core$buildDistanceTables()
  return(r5r_core)
}

#' récupère le setup du système de routing r5
#'
#' @param data_path path
#' @param verbose par défaut, FALSE
#' @param temp_dir par défaut, FALSE
#' @param use_elevation par défaut, FALSE
#' @param overwrite par défaut, FALSE
#'
#'
#' @export
get_setup_r5 <- function (data_path, verbose = FALSE, temp_dir = FALSE,
                          use_elevation = FALSE, overwrite = FALSE) {
  
  rlang::check_installed("rJava", reason = "rJava est nécessaire pour r5r")
  rlang::check_installed("r5r", reason = "r5r est nécessaire pour ce calcul")
  
  checkmate::assert_directory_exists(data_path)
  checkmate::assert_logical(verbose)
  checkmate::assert_logical(temp_dir)
  checkmate::assert_logical(use_elevation)
  checkmate::assert_logical(overwrite)
  rJava::.jinit()
  data_path <- path.expand(data_path)
  any_network <- length(grep("network.dat", list.files(data_path))) > 0
  
  if (!(any_network)) stop("\nAn network is needed")
  
  jars <- list.files(path=file.path(.libPaths()[1], "r5r", "jar")) |>
    purrr::keep(~ stringr::str_detect(.x,"^r5-[:graph:]*.jar"))
  jars_date <- stringr::str_extract(jars, pattern="[:digit:]{8}") |> as.numeric()
  jar <- jars[which.max(jars_date)]
  jar_file <- file.path(.libPaths()[1], "r5r", "jar", jar)
  r5r_jar <- file.path(.libPaths()[1], "r5r", "jar", "r5r_0_6_0.jar")
  dat_file <- file.path(data_path, "network.dat")
  
  return(list(r5r_jar = r5r_jar, r5_jar = jar, network = dat_file))
}

#' setup du système de routing de r5
#'
#' Cette fonction est utlisée pour fabriquer le "core" r5 qui est ensuite appelé pour faire le routage.
#' Elle utilise le r5.....jar téléchargé ou le télécharge si nécessaire.
#' Un dossier contenant les informations nécessaires doit être passé en parmètre.
#' Ce dossier contient au moins un pbf (OSM), des fichiers GTFS (zip), des fichiers d'élévation (.tif)
#' Si iol contient un network.dat, celui ci sera utilisé sans reconstruire le réseau
#'
#' @param path path Chemin d'accès au dossier
#' @param overwrite Regénére le network.dat même si il est présent
#' @param date date Date où seront simulées les routes
#' @param mode mode de transport, par défaut c("WALK", "TRANSIT"), voir r5r ou r5 pour les autres modes
#' @param montecarlo nombre de tirages montecarlo par minutes de time_windows par défaut 10
#' @param max_walk_time temps maximal à pied
#' @param time_window par défaut, 1. fenêtre pour l'heure de départ en minutes
#' @param percentiles par défaut, 50., retourne les percentiles de temps de trajet (montecarlo)
#' @param walk_speed vitesse piéton
#' @param bike_speed vitesse vélo
#' @param max_lts stress maximal à vélo (de 1 enfant à 4 toutes routes), 2 par défaut
#' @param max_rides nombre maximal de changements de transport
#' @param max_rows nombre maximale de lignes passées à detailled_itirenaries
#' @param n_threads nombre de threads
#' @param jMem taille mémoire vive, plus le nombre de threads est élevé, plus la mémoire doit être importante
#' @param quick_setup dans le cas où le core existe déjà, il n'est pas recréer, plus rapide donc, par défaut, FALSE
#' @param di renvoie des itinéraires détaillés (distance, nombre de branche) en perdant le montecarlo et au prix d'une plus grande lenteur
#' @param ext renvoie une travel_time_matrix plus détaillée  si di = FALSE (utilise le fonction expanded_travel_time_matrix de r5r au lieu de travel_time_matrix)
#' @param elevation_tif nom du fichier raster (WGS84) des élévations en mètre, en passant ce paramètre, on calcule le dénivelé positif.
#'                  elevatr::get_elev_raster est un bon moyen de le générer. Fonctionne même si on n'utilise pas les élévations dans le routing
#' @param elevation méthode de clcul (NONE, TOBLER, MINETTI )
#' @param dfMaxLength longueur en mètre des segments pour la discrétization
#'
#' @export
routing_setup_r5 <- function(path,
                             date="17-12-2019 8:00:00",
                             mode=c("WALK", "TRANSIT"),
                             montecarlo=10L,
                             max_walk_time= Inf,
                             time_window=1L,
                             percentiles=50L,
                             walk_speed = 5.0,
                             bike_speed = 12.0,
                             max_lts= 2,
                             max_rides= ifelse("TRANSIT"%in%mode, 3L, 1L),
                             elevation_tif = NULL,
                             elevation = "NONE",
                             dfMaxLength = 10,
                             breakdown = FALSE,
                             overwrite = FALSE,
                             n_threads= 4L,
                             max_rows=5000,
                             jMem = "12G",
                             ext = FALSE,
                             di = FALSE,
                             quick_setup = FALSE)
{
  env <- parent.frame()
  path <- glue::glue(path, .envir = env)
  rlang::check_installed("rJava", reason = "rJava est nécessaire pour r5r")
  rlang::check_installed("r5r", reason = "r5r est nécessaire pour ce calcul")
  assertthat::assert_that(
    all(mode%in%c('TRAM', 'SUBWAY', 'RAIL', 'BUS',
                  'FERRY', 'CABLE_CAR', 'GONDOLA', 'FUNICULAR',
                  'TRANSIT', 'WALK', 'BICYCLE', 'CAR', 'BICYCLE_RENT', 'CAR_PARK')),
    msg = "incorrect transport mode")
  
  mode_string <- stringr::str_c(mode, collapse = "&")
  rJava::.jinit(force.init = TRUE, silent=TRUE) #modif du code ci-dessus (MP)
  r5r::stop_r5()
  #rJava::.jgc(R.gc = TRUE)
  
  if(quick_setup)
    core <- quick_setup_r5(data_path = path)
  else {
    out <- capture.output(
      core <- r5r::setup_r5(data_path = path, verbose=FALSE,
                            elevation=elevation, overwrite = overwrite)
    )
  }
  
  
  options(r5r.montecarlo_draws = montecarlo)
  setup <- get_setup_r5(data_path = path)
  mtnt <- lubridate::now()
  type <- case_when(di ~ "r5_di",
                    ext & !di ~ "r5_ext",
                    TRUE ~ "r5")
  list(
    type = type,
    di = di,
    path = path,
    string = glue::glue("{type} routing {mode_string} sur {path} a {mtnt}"),
    core = core,
    montecarlo = as.integer(montecarlo),
    time_window = as.integer(time_window),
    departure_datetime = as.POSIXct(date, format = "%d-%m-%Y %H:%M:%S", tz=Sys.timezone()),
    mode = mode,
    percentiles = percentiles,
    max_walk_time = max_walk_time,
    walk_speed = walk_speed,
    bike_speed = bike_speed,
    max_rides = max_rides,
    max_lts = max_lts,
    elevation = elevation,
    elevation_tif = elevation_tif,
    pkg = c("r5r", "rJava"),
    dfMaxLength = dfMaxLength,
    breakdown = breakdown,
    elevation_data = if(is.null(elevation_tif)) NULL else terra::rast(str_c(path, "/", elevation_tif)),
    max_rows = max_rows,
    n_threads = as.integer(n_threads),
    future = TRUE,
    jMem = jMem,
    r5r_jar = setup$r5r_jar,
    r5_jar = setup$r5_jar,
    future_routing = function(routing) {
      rout <- routing
      rout$elevation_data <- NULL
      return(rout)},
    core_init = function(routing){
      options(java.parameters = glue::glue('-Xmx{routing$jMem}'))
      rJava::.jinit(silent=TRUE)
      r5r::stop_r5()
      rJava::.jgc(R.gc = TRUE)
      options(r5r.montecarlo_draws = routing$montecarlo)
      core <- r5r::setup_r5(data_path = routing$path, verbose=FALSE,
                            elevation=routing$elevation)
      out <- routing
      out$core <- core
      if(!is.null(routing$elevation_tif))
        out$elevation_data <- terra::rast(str_c(routing$path, "/", routing$elevation_tif))
      return(out)
    })
}

#' setup du système de routing otp
#'
#' @param router info du serveur otp.
#' @param port par défaut, 8000.
#' @param memory par défaut, 8G.
#' @param rep chemin du repertoire.
#' @param date date des trajets.
#' @param mode mode de transit, par défaut c("WALK", "TRANSIT").
#' @param max_walk_time temps maximal à pied
#' @param precisionMeters précision demandée au serveur, par défaut 50m.
#'
#' @export
routing_setup_otpv1 <- function(
    router,
    port=8000,
    memory="8G",
    rep,
    date="12-17-2019 8:00:00",
    mode=c("WALK", "TRANSIT"),
    max_walk_time= 30,
    precisionMeters=50)
{
  s_now <- lubridate::now()
  mode_string <- stringr::str_c(mode, collapse = "&")
  list(
    type = "otpv1",
    string = glue::glue("otpv1 routing {mode_string} sur {router}(:{port}) a {s_now}"),
    otpcon = OTP_server(router=router, port=port, memory = memory, rep=rep),
    date = unlist(stringr::str_split(date, " "))[[1]],
    time = unlist(stringr::str_split(date, " "))[[2]],
    mode = mode,
    batch = FALSE,
    arriveBy = FALSE,
    walkReluctance = 2,
    maxWalkDistance = max_walk_dist,
    transferPenalty = 0,
    minTransferTime = 0,
    clampInitialWait = 0,
    offRoadDistanceMeters = 50,
    precisionMeters = precisionMeters,
    future = FALSE)
}

#' setup du système de routing osrm
#'
#' @param server port du serveur osrm, par défaut 5000.
#' @param profile mode de transport, par défaut "driving".
#' @param future calcul parallele, par défaut TRUE.
#'
#' @export
routing_setup_osrm <- function(
    server=5000,
    profile="driving",
    future=TRUE)
{
  s_now <- lubridate::now()
  list(
    type = "osrm",
    string = glue::glue("osrm routing localhost:{server} profile {profile} a {s_now}"),
    osrm.server = glue::glue("http://localhost:{server}/"),
    osrm.profile = profile,
    future = TRUE,
    pkg = "osrm",
    mode = switch(profile,
                  driving="CAR",
                  walk="WALK",
                  bike="BIKE"))
}

#' setup du système de routing euclidien
#'
#' @param mode mode de transport, par défaut, "WALK".
#' @param speed vitesse de déplacement, par défaut, 5km/h.
#'
#' @export
routing_setup_euc <- function(
    mode="WALK", speed=5)
{
  s_now <- lubridate::now()
  list(
    type = "euclidean",
    string = glue::glue("euclidien a {s_now}"),
    future = TRUE,
    mode = mode,
    speed = speed)
}

#' lancement d'un serveur java otp
#'
#' @param router nom du routeur
#' @param port par défaut, 8000
#' @param memory taille mémoire du serveur, par défaut 8G
#' @param rep répertoire
#' @export

OTP_server <- function(router="IDF1", port=8008, memory="8G", rep)
{
  safe_otp_connect <- purrr::safely(otpr::otp_connect)
  connected <- FALSE
  connection <- safe_otp_connect(router=router, port=port)
  if (!is.null(connection$error))
  {
    secureport <- port+1
    current.wd <- getwd()
    setwd("{rep}/otp_idf" |> glue::glue())
    shell("start java -Xmx{memory} -jar otp-1.4.0-shaded.jar --router {router} --graphs graphs --server --port {port} --securePort {secureport}"|> glue::glue(),
          translate = TRUE, wait = FALSE, mustWork = TRUE)
    setwd(current.wd)
    safe_otp_connect <- purrr::safely(otpr::otp_connect)
    connected <- FALSE
    while(!connected) {
      connection <- safe_otp_connect(router=router, port=port)
      connected <- is.null(connection$error)}
  }
  connection$result
}

#' setup du système de routing de dodgr
#'
#' Cette fonction met en place ce qui est nécessaire pour lancer dodgr
#' A partir d'un fichier de réseau (au format silicate, téléchargé par overpass, voir download_dodgr_osm)
#' le setup fabrique le weighted_streetnetwork à partir d'un profile par mode de transport
#' Ce fichier est enregistré est peut être ensuite utilisé pour calculer les distances ou les temps de parcours
#'
#' @param path string, chemin d'accès au dossier contenant le réseau
#' @param date string, date Date où seront simulées les routes (non utilisé)
#' @param mode string, mode de transport, par défaut "CAR" (possible (BICYCLE, WALK,...))
#' @param turn_penalty booléen, applique une pénalité pour les turns
#' @param distances booléen, calcule les distances en prime
#' @param wt_profile_file string, chemin vers le fichier des profils (écrit avec \code{dodgr::write_dodgr_wt_profile})
#' @param overwrite booléen, Regénére le reseau même si il est présent
#' @param n_threads entier, nombre de threads
#' @param overwrite reconstruit le réseau dodgr à partir de la source OSM
#'
#' @export
routing_setup_dodgr <- function(path,
                                date="17-12-2019 8:00:00",
                                mode="CAR",
                                turn_penalty = FALSE,
                                distances = FALSE,
                                wt_profile_file = NULL,
                                n_threads= 4L,
                                overwrite = FALSE)
{
  env <- parent.frame()
  path <- glue::glue(path, .envir = env)
  rlang::check_installed("dodgr", reason="requis pour le routage")
  assertthat::assert_that(
    mode%in%c("CAR", "BICYCLE", "WALK", "bicycle", "foot", "goods",
              "hgv", "horse", "moped",
              "motorcar", "motorcycle", "psv", "wheelchair"),
    msg = "incorrect transport mode")
  mode <- case_when(mode=="CAR"~"motorcar",
                    mode=="BICYCLE"~"bicycle",
                    mode=="WALK"~"foot")
  RcppParallel::setThreadOptions(numThreads = as.integer(n_threads))
  # dans le path on s'attend à 1 fichier sfosm
  loff <- list.files(path=path, pattern = "*.sfosm")
  if(length(loff)>1)
    message("Attention, il y a plusieurs candidats de réseaux dans le dossier {path}" |> glue::glue())
  graph_name <- stringr::str_c(stringr::str_remove(loff[[1]], "\\.[:alpha:]*$"), ".", mode, ".dodgrnet")
  graph_name <- glue::glue("{path}/{graph_name}")
  if(file.exists(graph_name)&!overwrite) {
    graph <- qs::qread(graph_name, nthreads=4)
    message("dodgr network en cache")
  } else {
    osm_network <- qs::qread(str_c(path, "/", loff[[1]]), nthreads=4)
    graph <- dodgr::weight_streetnet(osm_network, wt_profile=mode, wt_profile_file = wt_profile_file, turn_penalty = turn_penalty)
    qs::qsave(graph, graph_name, nthreads=4)
  }
  mtnt <- lubridate::now()
  type <- "dodgr"
  list(
    type = type,
    path = path,
    graph = graph,
    distances = distances,
    pkg = "dodgr",
    turn_penalty = turn_penalty,
    graph_name = graph_name,
    string = glue::glue("{type} routing {mode} sur {path} a {mtnt}"),
    departure_datetime = as.POSIXct(date, format = "%d-%m-%Y %H:%M:%S", tz=Sys.timezone()),
    mode = mode,
    n_threads = as.integer(n_threads),
    future = TRUE,
    future_routing = function(routing) {
      rout <- routing
      rout$graph <- NULL
      rout$elevation_data <- NULL
      return(rout)
    },
    core_init = function(routing){
      RcppParallel::setThreadOptions(numThreads = routing$n_threads)
      # refresh le graph à partir du disque en cas de multicore
      rout <- routing
      rout$graph <- qs::qread(routing$graph_name, nthreads=4)
      if(!is.null(routing$elevation))
        rout$elevation_data <- terra::rast(str_c(routing$path, "/", routing$elevation))
      return(rout)
    })
}

dodgr_ttm <- function(o, d, tmax, routing)
{
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  temps <- dodgr::dodgr_times(
    graph = routing$graph,
    from = o,
    to = d,
    shortest=FALSE)
  temps <- data.table(temps, keep.rownames = TRUE)
  temps[, fromId:=rn |> as.integer()] [, rn:=NULL]
  temps <- melt(temps, id.vars="fromId", variable.name="toId", value.name = "travel_time", variable.factor = FALSE)
  temps <- temps[travel_time<=tmax*60,]
  if(routing$distances) {
    dist <- dodgr::dodgr_distances(
      graph = routing$graph,
      from = o,
      to = d,
      shortest=FALSE,
      parallel = TRUE)
    dist <- data.table(dist, keep.rownames = TRUE)
    dist[, fromId:=rn |> as.integer()] [, rn:=NULL]
    dist <- melt(dist, id.vars="fromId", variable.name="toId", value.name = "distance", variable.factor = FALSE)
    temps <- merge(temps, dist, by=c("fromId", "toId"), all.x=TRUE, all.y=FALSE)
  }
  erreur <- NULL
  
  if (nrow(temps)>0)
    temps[, `:=`(fromId=as.integer(fromId), toId=as.integer(toId), travel_time=as.integer(travel_time/60))]
  else
  {
    erreur <- "dodgr::travel_time_matrix empty"
    temps <- data.table(fromId=numeric(), toId=numeric(), travel_time=numeric(), distance=numeric())
    logger::log_warn(erreur)
  }
  return(list(result=temps, error=erreur))
}





#' Calcul de l'accessibilité
#'
#' \code{iso_accessibilite} calcule l'accessibilité à des aménités définies sur une carte (sous forme de sf) et
#' renvoie un raster à une certaine résolution. Le calcul des distances isochroniques est organisé par secteurs et
#' est majoré pour évacuer des calculs inutiles entre secteurs éloignés.
#'
#' Les étapes du calcul peuvent être enregistrées pour ne pas repartir de zéro si le calcul plante en cours de route.
#' l'algorithme fonctionne comme cela :
#' 1. découpe les groupes d'origines
#' 2. par groupe
#'   1. prend un point au hasard
#'   2. calcule les distances entre ce point et les cibles
#'   3. calcule les distance entre le point et les autres du paquet
#'   4. sélectionne pour chaque auter point du paquet les cibles atteignables
#'   5. calcule les distances
#' 4. aggrege
#' 5. cumule
#' 6. rasterize
#'
#' @param quoi sf de variables numériques de différentes aménités, qui vont être agrégées au sein d'isochrones.
#'             Différentes options sont possibles pour le format et les formes passées en entrée.
#' @param ou positions sur lesquelles sont calculées les accessibilités.
#'           Si NULL, un raster est défini par défaut en ayant la même empreinte que les oportunités, à la résolution resolution.
#' @param res_quoi rasterisation éventuelle des lieux à la résolution donnée. Par défaut, reste un sf.
#' @param resolution résolution retenue pour l'accessibilité. Par défaut, 200m si ou est NULL, Inf sinon.
#' @param var_quoi si rasterisation, définie la fonction d'agrégation. Non implémenté.
#' @param routing défini le moteur de routage. Le routing dopit être initialisé par un setup_routing_*
#' @param tmax temps maximal pour les calculs d'isochrone. Par défaut, 10 mn.
#' @param pdt pas de temps pour le calcul des isochrones. Par defaut, incrément de 1mn.
#' @param chunk taille des paquets pour le compartimentage des calculs.
#' @param future calcul parallélisé. Par défaut, FALSE.
#' @param out format de sortie ("data.table", "sf", "raster"). Par défaut, raster.
#' @param ttm_out conserver travel_time_matrix. Par défaut, FALSE. Peut générer un objet volumineux.
#' @param logs dossier pour le fichier log, par defaut la variable localdata (non définie).
#' @param dir dossier des étapes du calcul (permet de récupérer ce qui a déjà calculé en cas de plantage).
#' @param table2disk enregistre les étapes du calcul. Par défaut, si dir est défini, TRUE.
#'
#' @return Par défaut, la sortie est une liste de bricks (ensemble de rasters). Chaque élément de la liste renvoie à une aménité et la brick donne tous les rasters selon le temps de déplacement.
#'
#' @import data.table
#' @import sf
#'
#' @export
#'

iso_accessibilite <- function(
    quoi,                            # sf avec des variables numériques qui vont être agrégées
    ou = NULL,                         # positions sur lesquelles sont calculés les accessibilités (si NULL, sur une grille)
    res_quoi = Inf,                    # projection éventuelle des lieux sur une grille
    resolution = ifelse(is.null(ou), 200, Inf),
    var_quoi = "individuals",          # si projection fonction d'agrégation
    routing = "r5",                         # défini le moteur de routage
    tmax = 10L,                        # en minutes
    pdt = 1L,
    chunk = 10000000,                   # paquet envoyé
    future = FALSE,
    out = ifelse(is.finite(resolution), resolution, "raster"),
    ttm_out = FALSE,
    logs = ".",
    dir = NULL,
    table2disk = if(is.null(dir)) FALSE else TRUE)    # ne recalcule pas les groupes déjà calculés, attention !
{
  start_time <- Sys.time()
  
  if(future)
    rlang::check_installed("furrr", reason="furrr est nécessaire pou rles cacluls en parallèle")
  
  dir.create(glue::glue("{logs}/logs"), showWarnings = FALSE, recursive = TRUE)
  timestamp <- lubridate::stamp("15-01-20 10h08.05", orders = "dmy HMS", quiet = TRUE) (lubridate::now(tzone = "Europe/Paris"))
  logfile <- glue::glue("{logs}/logs/iso_accessibilite.{routing$type}.{timestamp}.log")
  logger::log_appender(logger::appender_file(logfile))
  
  logger::log_success("Calcul accessibilite version 2")
  logger::log_success("{capture.output(show(routing))}")
  logger::log_success("")
  logger::log_success("tmax:{tmax}")
  logger::log_success("pdt:{pdt}")
  logger::log_success("chunk:{f2si2(chunk)}")
  logger::log_success("resolution:{resolution}")
  
  logger::log_success("out:{out}")
  
  opp_var <- names(quoi |>
                     dplyr::as_tibble()  |>
                     dplyr::select(where(is.numeric)))
  
  if (length(opp_var) == 0)
  {
    opp_var <- "c"
    quoi <- dplyr::mutate(quoi, c=1)
  }
  
  names(opp_var) <- opp_var
  
  logger::log_success("les variables sont {c(opp_var)}")
  
  # fabrique les points d'origine (ou) et les points de cibles (ou)
  # dans le système de coordonnées 4326
  
  ouetquoi <- iso_ouetquoi_4326(
    ou = ou,
    quoi = quoi,
    res_ou = resolution,
    res_quoi = res_quoi,
    opp_var = opp_var,
    fun_quoi = fun_quoi,
    resolution = resolution)
  
  ou_4326 <- ouetquoi$ou_4326
  quoi_4326 <- ouetquoi$quoi_4326
  
  npaires_brut <- as.numeric(nrow(quoi_4326)) * as.numeric(nrow(ou_4326))
  
  # établit les paquets (sur une grille)
  
  groupes <- iso_split_ou(
    ou = ou_4326,
    quoi = quoi_4326,
    chunk = chunk,
    routing = routing,
    tmax = tmax)
  
  ou_4326 <- groupes$ou
  ou_gr <- groupes$ou_gr
  k <- groupes$subsampling
  logger::log_success("{f2si2(nrow(ou_4326))} ou, {f2si2(nrow(quoi_4326))} quoi")
  logger::log_success("{length(ou_gr)} carreaux, {k} subsampling")
  
  if (table2disk)
    if (is.null(dir)) {
      dir <- tempdir()
      purrr::walk(ou_gr, ~file.remove(str_c(dir,"/", .x,".*")))
    }
  else
    if (!dir.exists(dir))
      dir.create(dir)
  
  message("...calcul des temps de parcours")
  pb <- progressr::progressor(steps=sum(groupes$Nous))
  nw <- future::nbrOfWorkers()
  logger::log_success("future:{future}, {nw} workers")
  
  packages <- c("data.table", "logger", "stringr", "glue", "raster", "terra", "qs", routing$pkg)
  
  if(routing$future & future) {
    if(!is.null(routing$core_init)) {
      pl <- future::plan()
      future::plan(pl)
      pids <- furrr::future_map(
        1:nw,
        ~ future:::session_uuid()[[1]])
      lt <- logger::log_threshold()
      splittage <- seq(0,length(ou_gr)-1) %/% ceiling(length(ou_gr)/nw)
      workable_ous <- split(ou_gr, splittage)
      routing <- routing$future_routing(routing)
      access <- furrr::future_map(workable_ous, function(gs) {
        logger::log_threshold(lt)
        logger::log_layout(logger::layout_glue_generator(
          format = "{level} [{pid}] [{format(time, \"%Y-%m-%d %H:%M:%S\")}] {msg}"))
        
        logger::log_appender(logger::appender_file(logfile))
        routing <- routing$core_init(routing)
        purrr::map(gs, function(g) {
          pb(amount=groupes$Nous[[g]])
          rrouting <- get_routing(routing, g)
          access_on_groupe(g, ou_4326, quoi_4326, rrouting, k, tmax, opp_var, ttm_out, pids, dir, t2d=table2disk)
        })
      }, .options=furrr::furrr_options(seed=TRUE,
                                       stdout=FALSE ,
                                       packages=packages)) |>
        purrr::flatten()
    }
    else {
      pl <- future::plan()
      future::plan(pl)
      pids <- furrr::future_map(
        1:nw,
        ~future:::session_uuid()[[1]])
      lt <- logger::log_threshold()
      routing <- routing$future_routing(routing)
      access <- furrr::future_map(
        ou_gr,
        function(g) {
          logger::log_threshold(lt)
          logger::log_appender(logger::appender_file(logfile))
          pb(amount=groupes$Nous[[g]])
          rrouting <- get_routing(routing, g)
          access_on_groupe(g, ou_4326, quoi_4326, rrouting, k, tmax, opp_var, ttm_out, pids, dir, t2d=table2disk)
        },
        .options=furrr::furrr_options(seed=TRUE, stdout=FALSE, packages=packages))
    }}
  else {
    pids <- future:::session_uuid()[[1]]
    access <- purrr::map(ou_gr, function(g) {
      pb(amount=groupes$Nous[[g]])
      rrouting <- get_routing(routing, g)
      access_on_groupe(g, ou_4326, quoi_4326, rrouting, k, tmax, opp_var, ttm_out, pids, dir, t2d=table2disk)
    })
  }
  
  access_names <- names(access)
  access <- rbindlist(access, use.names = TRUE, fill = TRUE)
  if(ttm_out)
  {
    message("...finalisation du routing engine")
    gc()
    pl <- future::plan()
    future::plan(pl) # pour reprendre la mémoire
    if(table2disk) {
      access <- purrr::map(set_names(access$file, access_names),~{
        tt <- qs::qread(.x, nthreads=4)
        if(is.null(tt)) return(NULL)
        tt[, .(fromId, toId, travel_time)]
        setkey(tt, fromId)
        setindex(tt, toId)
        tt
      }) |> purrr::compact()
      # if(length(access)>1)
      #   access <- rbindlist(access, use.names = TRUE)
    }
    npaires <- sum(map_dbl(access, nrow))
    res <- list(
      type = "dt",
      origin = routing$type,
      origin_string = routing$string,
      string = glue::glue("matrice de time travel {routing$type} precalculee"),
      time_table = access,
      fromId = ou_4326[, .(id, lon, lat, x, y, gr)],
      toId = quoi_4326[, .(id, lon, lat, x, y)],
      groupes = ou_gr,
      resolution = groupes$resINS,
      res_ou = resolution,
      res_quoi = res_quoi,
      ancres = FALSE,
      future =TRUE,
      mode = routing$mode)
  }
  else
  {
    if(table2disk)
      access <- rbindlist(
        purrr::map(access$file,~qs::qread(.x, nthreads=4)), use.names = TRUE, fill = TRUE)
    
    npaires <- sum(access[, .(npaires=npep[[1]]), by=fromId][["npaires"]])
    access[, `:=`(npea=NULL, npep=NULL)]
    
    groupes_ok <- unique(access$gr)
    groupes_pasok <- setdiff(ou_gr, groupes_ok)
    ou_pasok <- ou_4326[gr %chin% groupes_pasok, .N]
    
    logger::log_warn("{ou_pasok} ({signif(ou_pasok/nrow(ou_4326)*100, 1)}%) origines non evaluees ")
    access[, gr:=NULL]
    message("...cumul")
    setnames(access, new="temps", old="travel_time")
    timecrossou <- CJ(fromId=ou_4326$id,temps=c(0:tmax), sorted=FALSE)
    access <- merge(timecrossou, access, by=c("fromId", "temps"), all.x=TRUE)
    for (v in opp_var)
      set(access, i=which(is.na(access[[v]])), j=v, 0)
    setorder(access, fromId, temps)
    access_c <- access[, lapply(.SD, cumsum),
                       by=fromId,
                       .SDcols=opp_var]
    temps_c <- access[, .(temps=temps),by=fromId]
    access_c[,temps:=temps_c$temps]
    tt <- seq(pdt, tmax, pdt)
    access_c <- access_c[temps%in%tt, ]
    access_c <- merge(access_c, ou_4326, by.x="fromId", by.y="id")
    
    r_xy <- access_c[, .(x=x[[1]], y=y[[1]]), by=fromId] [, fromId:=NULL]
    
    if(is.numeric(out)) {
      outr <- resolution
      out <- "raster"
    }
    
    res <- switch(
      out,
      data.table = access_c,
      sf = access_c |>
        dplyr::as_tibble() |>
        st_as_sf(coords=c("x","y"), crs = 3035),
      raster = {
        message("...rasterization")
        ttn <- paste0("iso",tt, "m")
        ids <- idINS3035(ou_4326$x, ou_4326$y, resolution = outr)
        ids <- data.table(fromId = ou_4326$id, idINS200 = ids)
        purrr::map(opp_var, function(v) {
          r_xy <- data.table::dcast(access_c, fromId~temps, value.var=v)
          names(r_xy) <- c("fromId", ttn)
          r_xy <- merge(r_xy, ids, by="fromId")[, fromId:=NULL]
          raster::readAll(dt2r(r_xy, resolution=outr))
        })})
    
  }
  
  dtime <- as.numeric(Sys.time()) - as.numeric(start_time)
  red <- 100 * (npaires_brut - npaires) / npaires_brut
  tmn <- second2str(dtime)
  speed_b <- npaires_brut / dtime
  speed <- npaires / dtime
  mtime <- glue::glue("{tmn} - {f2si2(npaires)} routes - {f2si2(speed_b)} routes(brut)/s - {f2si2(speed)} routes/s - {signif(red,2)}% reduction")
  message(mtime)
  logger::log_success("{routing$string} en {mtime}")
  attr(res, "routing") <- glue::glue("{routing$string} en {mtime}")
  message("...nettoyage")
  future::plan() # remplace plan(plan())
  gc()
  
  res
}


f2si2 <- function (number, rounding=F, sep=" ") {
  lut <- c(1e-24, 1e-21, 1e-18, 1e-15, 1e-12, 1e-09, 1e-06, 
           0.001, 1, 1000, 1e+06, 1e+09, 1e+12, 1e+15, 1e+18, 1e+21, 
           1e+24)
  pre <- c("y", "z", "a", "f", "p", "n", "u", "m", "", "k", 
           "M", "B", "T", "P", "E", "Z", "Y")
  ix <- findInterval(number, lut)
  if (ix>0 && lut[ix]!=1) {
    if (rounding==T) {
      sistring <- paste(round(number/lut[ix], 1), pre[ix], sep=sep)
    } else {
      sistring <- paste(number/lut[ix], pre[ix], sep=sep)
    }
  } else {
    sistring <- as.character(number)
  }
  return(sistring)
}

# souci avec la fonction where, non exportée de tidyselect.
# la solution recommandée, au lieu de ::: est la suivante :
utils::globalVariables("where")