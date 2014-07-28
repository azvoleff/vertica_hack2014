library(stringr)
library(rgdal)
library(raster)
library(tools)

in_base <- c("H:/Data/Vertica_Hackathon/Spatial_Datasets")
in_folders <- c("Protected_Areas", "Roads", "TEAM_Sampling_Points", 
                "Zone_of_Interaction")
in_folders <- file.path(in_base, in_folders)

img_metadata <- read.csv("Vertica_hackathon_image_metadata.csv", 
                         stringsAsFactors=FALSE)

for (in_folder in in_folders) {
    orig_shps <- dir(in_folder, pattern=".shp$")
    orig_shps <- orig_shps[!grepl('WGS', orig_shps)]
    orig_shps <- orig_shps[!grepl('UTM', orig_shps)]
    print(length(orig_shps))

    for (this_shp in orig_shps) {
        sitecode <- str_extract(this_shp, "^[a-zA-Z]{2,3}")
        image_proj4string <- img_metadata[match(sitecode, img_metadata$sitecode), ]$proj4string

        this_data <- readOGR(in_folder, file_path_sans_ext(this_shp))

        this_data_wgs84 <- spTransform(this_data, CRS("+init=epsg:4326"))
        shp_wgs84_out <- paste0(file_path_sans_ext(this_shp), "_WGS84.shp")
        writeOGR(this_data_wgs84, in_folder, file_path_sans_ext(shp_wgs84_out), 
                 driver="ESRI Shapefile")
        kml_wgs84_out <- paste0(file_path_sans_ext(this_shp), "_WGS84.kml")
        writeOGR(this_data_wgs84, file.path(in_folder, kml_wgs84_out), 
                 file_path_sans_ext(kml_wgs84_out), driver="KML")

        this_data_utm <- spTransform(this_data, CRS(image_proj4string))
        shp_utm_out <- paste0(file_path_sans_ext(this_shp), "_UTM.shp")
        writeOGR(this_data_utm, in_folder, file_path_sans_ext(shp_utm_out), 
                 driver="ESRI Shapefile")
        
        shp_files <- dir(in_folder, pattern=paste0(file_path_sans_ext(this_shp), "[.]"))
        file.rename(file.path(in_folder, shp_files), file.path(in_folder, "ORIGINALS", shp_files))
    }
}
