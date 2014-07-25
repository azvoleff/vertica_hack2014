# Running this script on the Pasoh imagery takes about 1 hour.

library(foreach)
library(iterators)

library(stringr)
library(tools)
library(rgdal)
library(rgeos)
library(dplyr)

input_dir <- 'H:/Data/Landsat/LCLUC_Training'
out_dir <- 'H:/Data/Landsat/Training_targz_files'
#input_dir <- 'H:/Data/Landsat/LCLUC_Classifications'

training_files <- dir(input_dir, pattern='_Landsat_Training_Data.shp', 
                      full.names=TRUE)

img_metadata <- read.csv("Vertica_hackathon_image_metadata.csv", 
                         stringsAsFactors=FALSE)

timestamp()
retvals <- foreach (training_file=iter(training_files),
                    .packages=c("raster", "tools", "R.utils", "Rcpp", 
                                "inline", "BH"),
                    .inorder=FALSE) %do% {
    polys <- readOGR(dirname(training_file), 
                     file_path_sans_ext(basename(training_file)))

    sitecode <- str_extract(basename(training_file), '[a-zA-Z]*')
    image_proj4string <- img_metadata[match(sitecode, img_metadata$sitecode), 
                                      ]$proj4string
    polys <- spTransform(polys, CRS(image_proj4string))

    polydata <- polys@data
    polydata <- polydata[, order(names(polydata))]
    polydata <- cbind(poly_coords=writeWKT(polys, byid=TRUE), polydata)
    polydata <- melt(polydata, id.vars=c("poly_coords", "UUID"), 
                     measure.vars=grep('Class',names(polydata)), 
                     variable.name="year", value.name="class")

    # There should be a polygon for each year at this point
    stopifnot(length(polydata) != (5 * length(polys)))

    polydata$year <- as.numeric(gsub('Class_', '', polydata$year))
    polydata <- polydata[!polydata$class == "Unknown", ]
    polydata <- cbind(polyid=seq(0, nrow(polydata) - 1), polydata)

    txt_filename <- file.path(out_dir, paste0(file_path_sans_ext(basename(training_file)), ".txt"))
    write.table(polydata, file=txt_filename, row.names=FALSE, quote=FALSE, sep="|")
    gzip(txt_filename, remove=TRUE, overwrite=TRUE)
}
timestamp()
