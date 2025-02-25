library(knitr)
opts_chunk$set(
  fig.pos="htb", 
  out.extra="",
  dev="ragg_png",
  dev.args = list(bg = "transparent"),
  out.width="100%",
  fig.showtext=TRUE,
  message = FALSE,
  warning = FALSE,
  echo = FALSE)

library(tidyverse, quietly = TRUE)
library(ofce, quietly = TRUE)
library(showtext, quietly = TRUE)
library(gt, quietly = TRUE)
library(readxl, quietly = TRUE)
library(ggiraph, quietly = TRUE)
library(curl, quietly = TRUE)
library(ggrepel, quietly = TRUE)
library(gt, quietly = TRUE)
library(scales, quietly = TRUE)
library(glue, quietly = TRUE)
library(patchwork, quietly = TRUE)
library(downloadthis, quietly = TRUE)
library(lubridate, quietly = TRUE)
library(insee, quietly = TRUE)
library(ggh4x, quietly = TRUE)
library(PrettyCols, quietly = TRUE)
library(cli, quietly = TRUE)
library(quarto, quietly = TRUE)
library(qs, quietly = TRUE)
library(devtools, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(sf, quietly = TRUE)
library(tmap, quietly = TRUE)
library(mapdeck, quietly = TRUE)
library(marquee, quietly = TRUE)

options(
  ofce.base_size = 12,
  ofce.background_color = "transparent",
  ofce.source_data.src_in = "file",
  ofce.caption.ofce = FALSE,
  ofce.marquee = TRUE,
  ofce.caption.wrap = 0)

showtext_opts(dpi = 192)
showtext_auto()
options(cli.ignore_unknown_rstudio_theme = TRUE)
tooltip_css  <-  
  "font-family:Open Sans;
  background-color:snow;
  border-radius:5px;
  border-color:gray;
  border-style:solid; 
  border-width:0.5px;
  font-size:9pt;
  padding:4px;
  box-shadow: 2px 2px 2px gray;
  r:20px;"

gdtools::register_gfont("Open Sans")

girafe_opts <- function(x, ...) girafe_options(
  x,
  opts_hover(css = "stroke-width:1px;", nearest_distance = 60),
  opts_tooltip(css = tooltip_css, delay_mouseover = 10, delay_mouseout = 3000)) |> 
  girafe_options(...)

girafy <- function(plot, r=2.5, o = 0.5,  ...) {
  if(knitr::is_html_output()| interactive()) {
    girafe(ggobj = plot) |> 
      girafe_options(
        opts_hover_inv(css = glue("opacity:{o};")),
        opts_hover(css = glue("r:{r}px;")),
        opts_tooltip(css = tooltip_css)) |> 
      girafe_options(...)
  } else {
    plot
  }
}

milliards <- function(x, n_signif = 3L) {
  stringr::str_c(
    format(
      x, 
      digits = n_signif, 
      big.mark = " ",
      decimal.mark = ","),
    " milliards d'euros") 
}

if(.Platform$OS.type=="windows")
  Sys.setlocale(locale = "fr_FR.utf8") else
    Sys.setlocale(locale = "fr_FR")

margin_download <- function(data, output_name = "donnees", label = "données") {
  if(knitr::is_html_output()) {
    if(lobstr::obj_size(data)> 1e+5)
      cli::cli_alert("la taille de l'objet est supérieure à 100kB")
    fn <- tolower(output_name)
    downloadthis::download_this(
      data,
      icon = "fa fa-download",
      class = "dbtn",
      button_label  = label,
      output_name = fn)
  } else
    return(invisible(NULL))
}

margin_link <- function(data, output_name = "donnees", label = "données") {
  if(knitr::is_html_output()) {
    link <- stringr::str_c("dnwld/", output_name, ".csv")
    vroom::vroom_write(data, link, delim = ";")
    downloadthis::download_link(
      link,
      icon = "fa fa-download",
      class = "dbtn",
      button_label  = label)
  } else
    return(invisible(NULL))
}

margin_link2 <- function(data, output_name = "donnees", label = "données", force = FALSE) {
  if(knitr::is_html_output()|force) {
    link <- stringr::str_c("dnwld/", output_name, ".csv")
    vroom::vroom_write(data, link, delim = ";")
    htmltools::div(
      downloadthis::download_link(
        link,
        icon = "fa fa-download",
        class = "dbtn",
        button_label  = label),
      class = "column-margin")
  } else
    return(invisible(NULL))
}

inline_link <- function(link, label = "données") {
  if(knitr::is_html_output()) {
    le_link <- stringr::str_c("dnwld/", link, ".csv")
    downloadthis::download_link(
      le_link,
      icon = "fa fa-download",
      class = "dbtnlg",
      button_label  = label)
  } else
    return(invisible(NULL))
}

inline_download <- function(data, label = "données", output_name = "donnees") {
  downloadthis::download_this(
    data,
    icon = "fa fa-download",
    class = "dbtnlg",
    button_label  = label,
    output_name = output_name
  )
}

ccsummer <- function(n=4) PrettyCols::prettycols("Summer", n=n)
ccjoy <- function(n=4) PrettyCols::prettycols("Joyful", n=n)

bluish <- ccjoy()[1]
redish <- ccjoy()[2]
yelish <- ccsummer()[2]
greenish <- ccsummer()[4]
darkgreenish <- ccsummer()[3]
darkbluish <- ccjoy()[4]

out_graphes <- if(Sys.getenv("OUTGRAPHS") == "TRUE") TRUE else FALSE

date_trim <- function(date) {
  str_c("T", lubridate::quarter(date), " ", lubridate::year(date))
}

date_mois <- function(date) {
  str_c(lubridate::month(date,label = TRUE, abbr = FALSE), " ", lubridate::year(date))
}

date_jour <- function(date) {
  str_c(lubridate::day(date), " ", lubridate::month(date,label = TRUE, abbr = FALSE), " ", lubridate::year(date))
}

euro <- function(x, digits = 4) {
  str_c(formatC(x, digits = digits, big.mark = " ", decimal.mark = ",", format = "fg"), " €")
}

if(Sys.getenv("QUARTO_PROJECT_DIR") == "") {
  safe_find_root <- purrr::safely(rprojroot::find_root)
  root <- safe_find_root(rprojroot::is_quarto_project | rprojroot::is_r_package | rprojroot::is_rstudio_project)
  if(is.null(root$error))
    ofce.project.root <- root$result
} else {
  ofce.project.root <- Sys.getenv("QUARTO_PROJECT_DIR")
}

conflicted::conflicts_prefer(dplyr::filter, .quiet = TRUE)
conflicted::conflicts_prefer(dplyr::select, .quiet = TRUE)
conflicted::conflicts_prefer(dplyr::lag, .quiet = TRUE)
conflicted::conflicts_prefer(lubridate::year, .quiet = TRUE)
conflicted::conflicts_prefer(lubridate::month, .quiet = TRUE)
conflicted::conflicts_prefer(dplyr::first, .quiet = TRUE)
conflicted::conflicts_prefer(dplyr::last, .quiet = TRUE)
conflicted::conflicts_prefer(dplyr::between, .quiet = TRUE)
conflicted::conflicts_prefer(lubridate::quarter, .quiet = TRUE)

trim <- function(x, xm, xp) ifelse( x<= xm, xm, ifelse(x>= xp, xp, x))

communes <- bd_read("communes")
centre <- communes |> 
  filter(INSEE_COM=="13215") |> 
  st_transform(4326) |> 
  st_centroid() |> 
  st_coordinates()
centre <- as.vector(centre)

c200ze <- bd_read("c200ze") |> st_transform(4326)

tkn <- Sys.getenv("mapbox_token")
mapdeck::set_token(tkn)

style <- "mapbox://styles/xtimbeau/ckyx5exex000r15n0rljbh8od"


