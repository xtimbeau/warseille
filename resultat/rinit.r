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

knitr::knit_hooks$set(optipng = knitr::hook_optipng)

options(conflicts.policy = list(depends.ok=TRUE, error=FALSE, warn=FALSE, can.mask=TRUE))
library(tidyverse, quietly = TRUE)
library(scales, quietly = TRUE)
library(ofce, quietly = TRUE)
library(gt, quietly = TRUE)
library(ggiraph, quietly = TRUE)
library(ggrepel, quietly = TRUE)
library(gt, quietly = TRUE)
library(glue, quietly = TRUE)
library(patchwork, quietly = TRUE)
library(lubridate, quietly = TRUE)
library(insee, quietly = TRUE)
library(PrettyCols, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(sf, quietly = TRUE)
library(stars)
library(mapdeck, quietly = TRUE)
library(marquee, quietly = TRUE)

options(
  ofce.base_size = 12,
  ofce.background_color = "transparent",
  sourcoise.src_in = "file",
  sourcoise.init_fn = ofce::init_qmd,
  ofce.caption.ofce = FALSE,
  ofce.marquee = TRUE,
  ofce.caption.wrap = 0)

showtext::showtext_opts(dpi = 192)
showtext::showtext_auto()
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

conflicted::conflict_prefer_all("dplyr", quiet = TRUE)
conflicted::conflict_prefer_all("lubridate", quiet = TRUE)

trim <- function(x, xm, xp) ifelse( x<= xm, xm, ifelse(x>= xp, xp, x))

tkn <- Sys.getenv("mapbox_token")
mapdeck::set_token(tkn)

style <- "mapbox://styles/xtimbeau/ckyx5exex000r15n0rljbh8od"

tabsetize <- function(list, facety = TRUE, cap = TRUE, girafy = TRUE) {
  if(knitr::is_html_output()) {
    chunk <- knitr::opts_current$get()
    label <- knitr::opts_current$get()$label
    
    if(cap) {
      if(is.null(label))
        return(list)
      cat(str_c(":::: {#", label, "} \n\n" ))
    }
    
    cat("::: {.panel-tabset} \n\n")
    purrr::iwalk(list, ~{
      cat(paste0("### ", .y," {.tabset} \n\n"))
      
      if(girafy)
        girafy(.x) |> htmltools::tagList() |> print()
      else
      {
        id <- str_c(digest::digest(.x), "-", .y)
        rendu <- knitr::knit(text = str_c("```{r, label ='", id ,"'}\n .x \n```"), quiet=TRUE)
        cat(rendu, sep="\n")
      }
      cat("\n\n") })
    cat(":::\n\n")
    if(cap) {
      cat(chunk$fig.cap)
      cat("\n\n")
      cat("::::\n\n")
    }
  } else {
    if(facety)
      patchwork::wrap_plots(list, ncol = 2) & theme_ofce(base.size=6)
    else
      list[[1]] |> print()
  }
}

tabsetize2 <- function(list, facety = TRUE, cap = TRUE, girafy = FALSE) {
  if(knitr::is_html_output()) {
    chunk <- knitr::opts_current$get()
    label <- knitr::opts_current$get()$label
    
    if(cap) {
      if(is.null(label))
        return(list)
      cat(str_c("::::: {#", label, "} \n\n" ))
    }
    
    cat(":::: {.panel-tabset} \n\n")
    purrr::iwalk(list, ~{
      cat(paste0("### ", .y," {.tabset} \n\n"))
      tabsetize(.x, facety=FALSE, cap = FALSE, girafy = girafy)
      cat("\n\n")
    })
    cat("::::\n\n")
    
    if(cap) {
      cat(chunk$fig.cap)
      cat("\n\n")
      cat(":::::\n\n")
    }
  } else {
    if(facety)
      patchwork::wrap_plots(list, ncol = 2) & theme_ofce(base.size=6)
    else
      list[[1]] |> print()
  }
}

download_margin <- function(data, output_name = "donnees", label = "donn\u00e9es", margin = TRUE) {
  
  if(knitr::is_html_output()) {
    if(lobstr::obj_size(data)> 1e+5)
      cli::cli_alert("la taille de l'objet est sup\u00e9rieure à 100kB")
    fn <- tolower(output_name)
    link <- stringr::str_c("dnwld/", output_name, ".csv")
    vroom::vroom_write(data, link, delim = ";")
    
    dwn <- downloadthis::download_link(
      link,
      icon = "fa fa-download",
      class = "dbtn",
      button_label  = label)
    
    cat(str_c("::: {.column-margin} \n" ))
    dwn |> htmltools::tagList() |> print()
    cat("\n")
    cat(":::\n")
    
  } else
    return(invisible(NULL))
}

