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

cl <- makeCluster(6)
registerDoParallel(cl)

input_dir <- 'D:/azvoleff/Data/Landsat/LCLUC_Classifications'
out_dir <- 'D:/azvoleff/Data/Landsat/Mosaic_targz_files'
#input_dir <- 'H:/Data/Landsat/LCLUC_Classifications'

image_files <- dir(input_dir,
                   pattern='_predictors(_masks)?.tif$', 
                   full.names=TRUE)

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
                                "inline", "BH"),
                    .inorder=FALSE) %dopar% {

    image_stack <- stack(image_file)
    txt_filename <- file.path(out_dir, paste0(file_path_sans_ext(basename(image_file)), ".txt"))
    of <- file(txt_filename, "wt")
    bs <- blockSize(image_stack)
    for (block_num in 1:bs$n) {
        image_dims <- c(bs$nrows[block_num], ncol(image_stack), 
                        nlayers(image_stack))
        image_bl <- array(getValuesBlock(image_stack, row=bs$row[block_num], 
                                         nrows=bs$nrows[block_num]),
                          dim=c(image_dims[1] * image_dims[2], image_dims[3]))

        # Calculate coordinates
        first_cell <- (bs$row[block_num] - 1) * ncol(image_stack) + 1
        last_cell <- first_cell + bs$nrows[block_num]*ncol(image_stack) - 1
        xy <- xyFromCell(image_stack, cell=seq(first_cell, last_cell))

        # Format text output
        out <- rast2txt(xy, image_bl, first_cell - 1)

        # The output columns for the predictor images are:
        # 'pixelnum', 'point', 'r1', 'r2','r3', 'r4','r5','r7', 'veg','vegmean', 
        # 'vegvar', 'vegdis', 'elev', 'slop', 'asp'

        cat(out, file=of, append=TRUE, sep="\n")
    }
    close(of)
    gzip(txt_filename, remove=TRUE)

}
timestamp()

message("Writing metadata...")
timestamp()
meta <- foreach (image_file=iter(image_files), .combine=rbind) %do% {
    image_stack <- stack(image_file)
    data.frame(sitecode=str_extract(basename(image_file), '^[a-zA-Z]*'),
               year=str_extract(basename(image_file), '[0-9]{4}'),
               imgtype=ifelse(grepl('mask', image_file), 'masks', 'predictors'),
               nrows=nrow(image_stack),
               ncols=ncol(image_stack),
               xmn=xmin(image_stack),
               xmx=xmax(image_stack), 
               ymn=ymin(image_stack),
               ymx=ymax(image_stack),
               nl=nlayers(image_stack),
               proj4string=proj4string(image_stack))
}
write.csv(meta, file="Vertica_hackathon_image_metadata.csv", row.names=FALSE)
timestamp()
