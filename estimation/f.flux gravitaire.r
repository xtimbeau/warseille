flux_grav_non_furness <- function(dist, fuite, hab, emp, delta) {
  f <-  (1-fuite) * hab * exp(-dist/delta)
  f <- t(t(f)*emp)
  f1 <- f * (1-fuite) * hab / matrixStats::rowSums2(f, na.rm=TRUE)
  f1[is.na(f1)] <- 0
  return(f1)
}

flux_grav_furness <- function(dist, fuite, hab, emp, delta, tol=0.0001) {
  habf <- (1-fuite)*hab
  d0 <-  exp(-dist/delta)
  td0 <- t(exp(-dist/delta))
  # f <- t(t(f)*emp)
  ai0 <- 1/matrixStats::rowSums2(t(td0 * emp), na.rm=TRUE)
  bj0 <- rep(1,length(emp)) 
  err <- 1
  erri <- 0
  while(abs(err-erri)>tol) {
    err <- erri
    bj <- 1/matrixStats::colSums2( ai0 * habf * d0, na.rm=TRUE)
    part_e <- t(td0 * emp * bj)
    ai <- 1/matrixStats::rowSums2(part_e, na.rm=TRUE) 
    fi <- matrixStats::rowSums2(ai * habf * part_e, na.rm=TRUE)
    erri <- sqrt(mean((fi-habf)^2, na.rm=TRUE) )
    ai0 <- ai
    bj0 <- bj
  }
  res <- t(t(ai0 * habf * d0) * emp * bj0)
  res[is.na(res)] <- 0
  return(res)
}

flux_grav <- function(dist, fuite, hab, emp, delta, furness = FALSE, tol=0.0001) {
  if (furness)  {
    flux <- flux_grav_furness(dist, fuite, hab, emp, delta, tol) }
  else
    flux <- flux_grav_non_furness(dist, fuite, hab, emp, delta)
  return(flux)
}