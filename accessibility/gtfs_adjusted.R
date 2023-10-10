
# on modifie la fonction choisir_jour_transit avec la possibilité de prolonger lorsque les GTFS ne sont
# pas synchrones

choisir_jour_transit <- function(plage, start_time = "8:00:00") {
  jours <- plage[1]:plage[2] |> 
    as_date() |> 
    discard( ~ wday(.x) == 1 | wday(.x) == 7)
  jours <- jours[ceiling(length(jours) / 2)]
  
  return(ymd_hms(paste(jours, start_time)))
}

choix_jour <- function(directory) {
  les_gtfs <- list.files(directory, pattern = "*gtfs*|*GTFS*") |> 
    map( ~ read_gtfs(str_c(directory, .x, sep = "/")))
  
  les_jours <- map_dfr(les_gtfs, ~ {
    if (!("calendar" %in% names(.x)) ) {
      tmp <- .x$calendar_dates |> 
        filter(exception_type == 1) |> 
        group_by(date) |> 
        summarise(n = n()) |> 
        filter(wday(date) %in% 2:6) |> 
        arrange(-n)
      
      return(tmp |> slice(1) |> transmute(min = date, max = date))
    }
    
    return(data.frame(min = do.call(max, map(les_gtfs, ~ min(.x$calendar$start_date))),
                      max = do.call(min, map(les_gtfs, ~ max(.x$calendar$end_date)))))
  })
  
  print(les_jours)
  
  les_jours <- les_jours |> 
    summarise(min = max(min), max = min(max))
  
  return(c(les_jours[[1, "min"]], les_jours[[1, "max"]]))
}


plage <- function(directory) {
  les_gtfs <- list.files(directory, pattern = "*gtfs*|*GTFS*") |> 
    map( ~ read_gtfs(str_c(directory, .x, sep = "/")))
  
  les_plages <- map_dfr(les_gtfs, get_plage_in_calendar) |> 
    summarise(debut = max(min.date), fin = min(max.date)) |> 
    unlist()
  
  if (les_plages["debut"] > les_plages["fin"]) {
    print("Les gtfs ne sont pas synchrones, on utilise une version ajustée")
    
    for (k in 1:length(les_gtfs)) {
      gtfs <- les_gtfs[k]
      debut <- min(gtfs[[1]]$calendar$start_date, na.rm = TRUE)
      fin <- max(gtfs[[1]]$calendar$start_date, na.rm = TRUE)
      gtfs[[1]]$calendar$start_date <- debut
      gtfs[[1]]$calendar$end_date <- fin
      }
    
    les_plages <- map_dfr(les_gtfs, get_plage_in_calendar) |> 
      summarise(debut = max(min.date), fin = min(max.date)) |> 
      unlist()
    
    les_plages
    
  } else {
    les_plages
  }
}

get_plage_in_calendar <- function(gtfs) {
  
  if(is.null(gtfs[["calendar"]])) {
    plage <- data.frame(min.date = min(gtfs[["calendar_dates"]]$date, na.rm = TRUE),
                        max.date = max(gtfs[["calendar_dates"]]$date, na.rm = TRUE))
  } else {
    plage <- data.frame(min.date = min(gtfs[["calendar"]]$start_date, na.rm = TRUE),
                        max.date = max(gtfs[["calendar"]]$start_date, na.rm = TRUE))
  }
}