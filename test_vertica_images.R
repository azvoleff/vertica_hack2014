# Script to test basic handling of raster images with distributedR. Loads 
# imagery from a Vertica database.
#
# Alex Zvoleff, azvoleff@conservation.org, July 2014

Sys.setenv(VERTICAINI="/home/alexz/vertica.ini")
Sys.setenv(ODBCINI="/home/alexz/odbc.ini")

library(distributedR)
library(vRODBC)

id_cols <- c("pixelid",
             "datayear")
pred_cols <- c("r1", "r2", "r3", "r4", "r5", "r7", "veg", "vegmean", "vegvar", 
               "vegdis", "elev", "slop", "asp")

library(rgdal)
library(rgeos)
