#' convertit les idINS longs en courts
#'
#' @param idINS les id longs
#'
#' @return des id courts (100000*x/r + y/r)
#' @export
#'
contract_idINS <- function(idINS) {
  cr_pos <- stringr::str_locate(idINS[[1]], "r(?=[0-9])")[,"start"]+1
  cy_pos <- stringr::str_locate(idINS[[1]], "N(?=[0-9])")[,"start"]+1
  cx_pos <- stringr::str_locate(idINS[[1]], "E(?=[0-9])")[,"start"]+1
  lcoord <- cx_pos-cy_pos-1
  r <- as.numeric(stringr::str_sub(idINS[[1]],cr_pos,cy_pos-cr_pos))
  y <- as.numeric(stringr::str_sub(idINS,cy_pos,cy_pos+lcoord)) %/% r
  x <- as.numeric(stringr::str_sub(idINS,cx_pos,cx_pos+lcoord)) %/% r
  x * 100000 + y
} 

#' convertit les idINS courts en longs
#'
#' @param ids les id courts
#' @param resolution la résolution (defaut 200m)
#'
#' @return un vecteur d'idINS longs (ex. r200N2051600E4258800)
#' @export
#'
expand_idINS <- function(ids, resolution=200) {
  x <- round(ids/100000) 
  y <- round(resolution*(ids-x*100000))
  x <- resolution*x
  stringr::str_c("r", resolution, "N", y, "E", x)
}

#' Convertit l'idINS en polygone
#'
#' @param ids vecteur d'idINS.
#' @param resolution resolution, par défaut celle attachée à idINS.
#'
#' @export
#'
sidINS2square <- function(ids, resolution=200)
{
  x <- round(ids/100000) 
  y <- round(resolution*(ids-x*100000))
  x <- resolution*x
  purrr::pmap(list(x,y), ~sf::st_polygon(
    list(matrix(
      c(..1, ..1+resolution,
        ..1+resolution,..1,
        ..1, ..2,
        ..2, ..2+resolution,
        ..2+resolution,..2),
      nrow=5, ncol=2))))  |>
    sf::st_sfc(crs=3035)
}

#' Calculate euclidean distance between to short idINS, in meter
#'
#' @param fromidINS character vector of starting idINS
#' @param toidINS character vector of ending idINS
#' @param resolution default to NULL. Set if no resolution is provided in idINS
#'
#' @export
sidINS2dist <- function(fromidINS, toidINS, resolution=200) {
  stopifnot(length(fromidINS)==length(toidINS))
  if(length(fromidINS)==0)
    return(numeric())
  
  fromx <- round(fromidINS/100000) 
  fromy <- round((fromidINS-fromx*100000))
  fromx <- fromx
  
  tox <- round(toidINS/100000) 
  toy <- round((toidINS-tox*100000))
  tox <- tox
  
  return(resolution*sqrt((tox-fromx)^2 + (toy-fromy)^2))
}

#' retourne un sf à partir d'un data.frame avec un champ idINS
#'
#' @param data le data frame
#' @param resolution 200m par défaut
#' @param idINS le champ qui contient l'idINS court (défaut "idINS")
#'
#' @return un sf
#' @export
#'
sidINS2sf <- function(data, resolution = 200, idINS = "idINS") {
  data$geometry <- data[[idINS]] |> 
    sidINS2square()
  sf::st_as_sf(data)
}
