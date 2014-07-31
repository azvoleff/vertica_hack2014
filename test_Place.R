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

con <- odbcConnect("hack14")

pred_cols_paste <- paste(pred_cols, collapse=", ")
a <- sqlQuery(con, "SELECT STV_Create_Index(polyid, poly_coords USING PARAMETERS index='tr_polys') OVER() FROM CI.PSH_landsat")
b <- sqlQuery(con, "SELECT STV_Intersect(rowid, pixel_coord USING PARAMETERS index='tr_polys') OVER() AS (rowid, polyid) FROM CI.PSH_predictor")
c <- sqlQuery(con, paste("SELECT", pred_cols_paste, "STV_Intersect(rowid, pixel_coord USING PARAMETERS index='tr_polys') OVER() AS (rowid, polyid) FROM CI.PSH_predictor")
