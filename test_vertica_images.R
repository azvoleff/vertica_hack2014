# Script to test basic handling of raster images with distributedR. Loads 
# imagery from a Vertica database.
#
# Alex Zvoleff, azvoleff@conservation.org, July 2014

Sys.setenv(VERTICAINI="/home/alexz/vertica.ini")
Sys.setenv(ODBCINI="/home/alexz/odbc.ini")

library(tools)
library(raster)
library(vRODBC)

out_dir <- "/vertica/data/datasets/test_vertica_images"
stopifnot(file_test("-d", out_dir))

rasterOptions(tmpdir=file.path(out_dir))

###############################################################################
# Make funnction to write raster from vertica to a geotiff file for offline 
# viewing.
###############################################################################

# Write an image from a vertica table to a raster file
# @param sitecode a 2 or 3 letter code identifying a TEAM site
# @param year year of collection of desired imagery
# @param cols names of desired table columns
# @param filename output filename
# @param ... additional arguments as for \code{\link{writeRaster}}
vertica2tif <- function(sitecode, year, cols, filename, ...) {
    con <- odbcConnect("hack14")
    meta_qry <- paste0("select * from CI.spatial_ref where datayear=",
                        year, " and imgtype='predictors' and sitecode='",
                        sitecode, "'")
    img_meta <- sqlQuery(con, meta_qry, stringsAsFactors=FALSE)
    image_out <- with(img_meta, brick(nrows=nrows, ncols=ncols, xmn=xmn, 
                                      xmx=xmx, ymn=ymn, ymx=ymx,
                                      nl=length(cols), crs=CRS(proj4string)))
    cols_paste <- paste(cols, collapse=",")
    if (missing(filename)) filename <- rasterTmpFile()
    image_out <- writeStart(image_out, filename=filename, ...)
    bs <- blockSize(image_out)
    n_pix <- 0
    for (blocknum in 1:bs$n) {
        start_pixelid <- (bs$row[blocknum] - 1) * ncol(image_out)
        end_pixelid <- (bs$nrows[blocknum]) * ncol(image_out) + start_pixelid - 1
        img_qry <- paste0("select ", cols_paste, " from CI.", sitecode,
                          "_predictor where datayear=", year,
                          " and pixelid>= ", start_pixelid,
                          " and pixelid<= ", end_pixelid,
                          " order by pixelid")
        img_data <- as.matrix(sqlQuery(con, img_qry))
        image_out <- writeValues(image_out, v=img_data, start=bs$row[blocknum])
        n_pix <- n_pix + nrow(img_data)
    }
    image_out <- writeStop(image_out)
    return(image_out)
}

sitecodes <- read.csv("sitecode_key.csv")
#epochs <- c(1990, 1995, 2000, 2005, 2010)
epochs <- c(2000)

# sitecode <- "CAX"
# year <- 1990
# cols <- pred_cols
# filename <- "cax_1990.tif"
#
# cax_1990 <- vertica2tif("CAX", 1990, pred_cols,
#                         file.path(out_dir, "cax_1990.bil"),
#                         datatype="INT2S", format="BIL")

pred_cols <- c("r2", "r3", "r4")
for (sitecode in sitecodes$sitecode) {
    for (epoch in epochs) {
        message(paste(sitecode, epoch))
        filename <- file.path(out_dir, paste0(sitecode, "_", epoch, ".bil"))
        img_out <- vertica2tif(sitecode, epoch, pred_cols, filename,
                               datatype="INT2S", format="BIL")
        out_files <- dir(out_dir,
                         pattern=file_path_sans_ext(basename(filename)))
        bzfile <- paste0(file_path_sans_ext(filename), ".tar.bz2")
        system(paste("tar", "-jcvf", bzfile, "-C", out_dir,
                     paste(out_files, collapse=" ")))
        unlink(file.path(out_dir, out_files))
    }
}


