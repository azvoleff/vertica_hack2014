library(stringr)
library(rgdal)
library(raster)
library(tools)

in_base <- c("H:/Data/Landsat/Vertica_Hackathon/Spatial_datasets")
in_folders <- c("Protected_Areas", "Roads", "TEAM_Sampling_Points", 
                "Zone_of_Interaction")
in_folders <- file.path(in_base, in_folders)

img_metadata <- read.csv("Vertica_hackathon_image_metadata.csv", 
                         stringsAsFactors=FALSE)

for (in_folder in in_folders) {
    orig_shps <- dir(in_folder, pattern=".shp$")

    stopifnot(length(orig_shps) == 16)
    
    for (this_shp in orig_shps) {
        sitecode <- str_extract(this_shp, "^[a-zA-Z]{2,3}")
        image_proj4string <- img_metadata[match(sitecode, img_metadata$sitecode), 
                                          ]$proj4string

        this_data <- readOGR(in_folder, file_path_sans_ext(this_shp))

        site_utm_zone <- 

        this_data_wgs84 <- spTransform(this_data, CRS("+init=epsg:4326"))

        shp_wgs84_out <- paste0(file_path_sans_ext(this_shp), "_WGS84.shp")
        shp_utm_out <- paste0(file_path_sans_ext(this_shp), "_UTM.shp")
        # Save WGS84 and UTM in both shp and kml
        kml_wgs84_out <- paste0(file_path_sans_ext(this_shp), "_WGS84.kml")
        kml_utm_out <- paste0(file_path_sans_ext(this_shp), "_UTM.kml")
        
        file.rename(this_shp, file.path(in_folder, "ORIGINALS", this_shp))
    }

}

sitecodes <- str_extract(orig_shps, "^[a-zA-Z]{2,3}")
