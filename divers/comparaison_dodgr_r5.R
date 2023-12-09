#-----------------comparaison des parquet dodgr et r5-----------
library(arrow)
library(ggplot2)

bike_dodgr <- arrow::read_parquet('/space_mounts/data/marseille/distances/bike.parquet')
bike_r5 <- arrow::read_parquet('~/files/bike.parquet')

length(bike_dodgr$travel_time)
length(bike_r5$travel_time)

summary(bike_dodgr$travel_time)
summary(bike_r5$travel_time)

plot((hist(bike_dodgr$travel_time)), col= 'red')
plot(hist(bike_r5$travel_time), add=TRUE, col='blue')