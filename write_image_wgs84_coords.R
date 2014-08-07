# Running this script on the Pasoh imagery takes about 1 hour.

library(Rcpp)
library(inline)
library(BH)
library(raster)
library(stringr)
library(tools)
library(R.utils)

library(foreach)
library(iterators)
library(doParallel)

cl <- makeCluster(10)
registerDoParallel(cl)

#input_dir <- 'H:/Data/Landsat/Composites/Predictors'
#out_dir <- 'H:/Data/Landsat/Vertica_Hackathon/Mosaic_targz_files'
input_dir <- 'D:/azvoleff/Data/Landsat/Composites/Predictors'
out_dir <- 'D:/azvoleff/Data/Landsat/Vertica_Hackathon/Mosaic_targz_files'

image_files <- dir(input_dir,
                   pattern='_predictors(_masks)?.tif$')

# Only need one image_file per site
sitecodes <- str_extract(image_files, '^[a-zA-Z]*')
image_files <- image_files[match(unique(sitecodes), sitecodes)]

init_worker <- function() {
    library(Rcpp)
    sourceCpp('rast2txt.cpp')
    return(1)
}

# Need to compile the rast2txt function on each node in cluster

stopifnot(all(clusterCall(cl, init_worker), 1))

timestamp()
retvals <- foreach (image_file=iter(image_files),
                    .packages=c("raster", "tools", "R.utils", "Rcpp", 
                                "inline", "BH", "stringr"),
                    .inorder=FALSE) %dopar% {
    sitecode <- str_extract(image_file, '^[a-zA-Z]*')
    image_file <- file.path(input_dir, image_file)
    image_stack <- stack(image_file)
    txt_filename <- file.path(out_dir, paste0(sitecode, "_wgs84_coords.txt"))
    of <- file(txt_filename, "wt")
    bs <- blockSize(image_stack)
    for (block_num in 1:bs$n) {
        image_dims <- c(bs$nrows[block_num], ncol(image_stack), 
                        nlayers(image_stack))
        # Calculate coordinates
        first_cell <- (bs$row[block_num] - 1) * ncol(image_stack) + 1
        last_cell <- first_cell + bs$nrows[block_num]*ncol(image_stack) - 1
        xy <- xyFromCell(image_stack, cell=seq(first_cell, last_cell))

        # Transform to WGS84
        xy <- SpatialPoints(xy, proj4string=CRS(proj4string(image_stack)))
        xy <- spTransform(xy, CRS("+init=epsg:4326"))
        xy <- coordinates(xy)

        # Format text output
        out <- coords2txt(xy, first_cell - 1)

        # The output columns for the predictor images are:
        # 'pixelnum', 'point'
        cat(out, file=of, append=TRUE, sep="\n")
    }
    close(of)
    gzip(txt_filename, remove=TRUE)
}
timestamp()
