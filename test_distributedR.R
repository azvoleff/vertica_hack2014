# Script to test basic handling of raster images with distributedR. Loads 
# imagery from a Vertica database.
#
# Alex Zvoleff, azvoleff@conservation.org, July 2014

library(distributedR)
library(vRODBC)
#distributedR_start()
Sys.setenv(VERTICAINI="/home/alexz/vertica.ini")
Sys.setenv(ODBCINI="/home/alexz/odbc.ini")

distributedR_start(cluster_conf='/opt/hp/distributedR/conf/cluster.xml')

con <- odbcConnect("hack14")

###############################################################################
# Test db2darray
###############################################################################


###############################################################################
# Test loading a single image into distributedR darray
###############################################################################

# Get number of rows in a single image
n_rows <- sqlQuery(con, "select count(*) from CI.pasoh_predictor where datayear=1990")
# Build darray with this n_rows rows, and distribute in blocks of 100000 rows
img_1990 <- darray(dim=c(n_rows, 15), blocks=c(1e5, 15), sparse=FALSE)

# Load an image into a matrix using vRODBC
img_1990 <- sqlQuery(con, "select * from CI.pasoh_predictor where datayear=1990 order by pixelnum")

# Build darray
foreach(i, 1:npartitions(img_1990),
    init_img1990 <- function(x=splits(img_1990, i), index=i) {
        library(vRODBC)
        con <- odbcConnect("hack14")
        #size <- nrow(x)
        size <- 100000
        start_row <- (index-1) * size
        end_row <- index * size
        qry <- paste("select * from CI.pasoh_predictor where pixelnum >=", 
                     start_row, "and pixelnum <", end_row, " and datayear=1990 
                     order by pixelnum")
        x <- as.matrix(sqlQuery(con, qry))
        odbcClose(con)
        update(x)
    })

# View darray output:
head(getpartition(img_1990, 1))

###############################################################################
# Test calculating mean surface reflectance across all images in the database
###############################################################################

# Get number of rows in a single image
n_rows <- as.numeric(sqlQuery(con, "select count(*) from CI.pasoh_predictor where datayear=1990"))
# Get number of years of imagery in database
n_years <- as.numeric(sqlQuery(con, "select count(distinct datayear) from CI.pasoh_predictor"))

# Build darray with n_years*n_rows rows. Define number of rows per partition of 
# darray in terms of a single image - so each block will actually have 
# blockrows * n_years rows of data in it, since each block has multiple images. 
# Note that bs_per_img needs to be a multiple of the number of columns in the 
# raster itself (2088) so that it can be easily written out block-by-block by 
# writeRaster.
bs_per_img <- 2e5
bs_per_img <- bs_per_img - (bs_per_img %% 2088)
all_imgs <- darray(dim=c(n_rows*n_years, 15),
                   blocks=c(bs_per_img*n_years, 15), sparse=FALSE)

# Build darray including all images. Need to ensure each block of darray 
# includes matched rows from the same pixels do this with SQL "order by".
#
# Note that there isn't really a need to load all the data into a darray first 
# as I do below. It would be faster to run the calculations directly in the 
# below foreach loop, and to then save the results into a darray.  Loading all 
# the data into a darray and performing the calculations separately is only 
# useful if there are further calculations to perform on the data.
foreach(i, 1:npartitions(all_imgs),
    init_allimgs <- function(x=splits(all_imgs, i), index=i) {
        library(vRODBC)
        con <- odbcConnect("hack14")
        bs_per_img <- 2e5
        bs_per_img <- bs_per_img - (bs_per_img %% 2088)
        start_row <- (index-1) * bs_per_img
        end_row <- index * bs_per_img
        qry <- paste("select * from CI.pasoh_predictor where pixelnum >=", 
                     start_row, "and pixelnum <", end_row, "order by pixelnum,datayear")
        x <- as.matrix(sqlQuery(con, qry))
        odbcClose(con)
        update(x)
    })

# View darray output:
head(getpartition(all_imgs, 1))

# Calculate mean of reflectance bands only - so only need 7 bands.
mean_r <- darray(dim=c(n_rows, 7), blocks=c(bs_per_img, 7), sparse=FALSE)

foreach(i, 1:npartitions(all_imgs),
    calc_mean_r <-function(x=splits(all_imgs, i),
                           results=splits(mean_r, i),
                           index=i) {
    # Select out only the reflectance rows x[, 3:9]. x[, 2] is the pixelnum 
    # column.
    results <- as.matrix(aggregate(x[, 3:8], by=list(x[, 2]), FUN=mean, na.rm=TRUE))
    update(results)
})

# View darray output:
head(getpartition(mean_r, 1))

###############################################################################
# Write output mean surface reflectance image from distributedR darray to a 
# GeoTIFF file for viewing offline.
###############################################################################

library(raster)

# Write a darray to a raster file
# @param x a darray. The first column should be the pixel number, starting from
# zero, in row-major order.
# @param xcols the column numbers from x that should be included in the output 
# raster (excludes the pixelnum column by default).
# @param img_meta a data.frame with spatial referencing information for the 
# outptu raster.
# @param filename output filename
# @param ... additional arguments as for \code{\link{writeRaster}}
darray2tif <- function(x, xcols=c(2:ncol(x)), img_meta, filename, ...) {
    image_out <- with(img_meta, brick(nrows=nrows, ncols=ncols, xmn=xmn, 
                                      xmx=xmx, ymn=ymn, ymx=ymx, nl=nl, 
                                      crs=as.character(proj4string)))
    stopifnot(length(xcols) == nlayers(image_out))
    if (missing(filename)) filename <- rasterTmpFile()
    image_out <- writeStart(image_out, filename=filename, ...)
    # filename <- rasterTmpFile()
    # image_out <- writeStart(image_out, filename=filename)
    for (partnum in 1:npartitions(x)) {
        block <- getpartition(x, partnum)
        # Note that writeValues uses 1 based indexing, while pixel numbers are 
        # stored in a zero-based index
        pixel_num <- block[1, 1] + 1
        start_row <- (pixel_num - 1) / img_meta$ncols + 1
        image_out <- writeValues(image_out, v=block[, xcols], start=start_row)
    }
    image_out <- writeStop(image_out)
    return(image_out)
}

img_meta <- data.frame(nrows=2785, ncols=2088, xmn=173910, xmx=236550, 
                       ymn=309000, ymx=392550, nl=6,
                       proj4string='+proj=utm +zone=48 +datum=WGS84 +units=m +no_defs +ellps=WGS84 +towgs84=0,0,0')
mean_r_rast <- darray2tif(mean_r, xcols=c(2:ncol(mean_r)), img_meta=img_meta, 
                          filename='PSH_mean_r_test.tif', datatype='INT2S', 
                          overwrite=TRUE)

