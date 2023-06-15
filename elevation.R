#package happign is the interface to IGN (elevation)

library(happign)
library(sf)
library(tmap)

# shape from the best town in France
marseille <- read_sf("contours_quartiers_Marseille.shp")

# For quick testing, use interactive = TRUE
raster <- get_wms_raster(shape = marseille, interactive = TRUE)

# For specific use, choose apikey with get_apikey() and layer_name with get_layers_metadata()
#apikey <- get_apikeys()[4]  # altimetrie
#metadata_table <- get_layers_metadata(apikey, "wms") # all layers for altimetrie wms
#layer_name <- as.character(metadata_table[2,1]) # ELEVATION.ELEVATIONGRIDCOVERAGE

# Downloading digital elevation model from IGN
mnt <- get_wms_raster(marseille, apikey = "q6ti3et97vibqkfitsyx71bb", resolution = 25)

#if we want to plot
# Preparing raster for plotting
mnt[mnt < 0] <- NA # remove negative values in case of singularity
names(mnt) <- "Elevation [m]" # Rename raster ie the title legend

# Plotting result
tm_shape(mnt)+
  tm_raster(legend.show = FALSE)+
  tm_shape(marseille)+
  tm_borders(col = "blue", lwd  = 3)
