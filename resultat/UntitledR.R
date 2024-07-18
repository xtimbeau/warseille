library(arrow)
library(tidyverse)
library(duckplyr)
dd <- tibble(a = 1:100, b = letters[as.integer(ceiling(10*runif(100)))] )
vroom::vroom_write(dd, "dd.csv" , delim = ",")
open_dataset("dd.csv", format = "csv") |>
  to_duckdb() |> 
  mutate(b = arrow::struct(b))

