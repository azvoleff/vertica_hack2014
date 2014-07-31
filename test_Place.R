# Script to test basic handling of raster images with distributedR. Loads 
# imagery from a Vertica database.
#
# Alex Zvoleff, azvoleff@conservation.org, July 2014

Sys.setenv(VERTICAINI="/home/alexz/vertica.ini")
Sys.setenv(ODBCINI="/home/alexz/odbc.ini")

library(distributedR)
library(vRODBC)
library(HPdclassifier) # for hpdrandomForest
library(HPdata) # for db2dframe

id_cols <- c("pixelid",
             "datayear")
pred_cols <- c("r1", "r2", "r3", "r4", "r5", "r7", "veg", "vegmean", "vegvar", 
               "vegdis", "elev", "slop", "asp")

con <- odbcConnect("hack14")

int_index_qry <- "
SELECT STV_Create_Index(polyid, poly_coords USING PARAMETERS index='tr_polys')
OVER()
FROM CI.PSH_landsat"
sqlQuery(con, int_index_qry)

join_cols <- paste(c(pred_cols, "class"), collapse=", ")
join_qry <- paste(
"SELECT", join_cols,
"FROM
(
    SELECT *
    FROM CI.PSH_predictor
    JOIN
    (
        SELECT STV_Intersect(rowid, pixel_coord USING PARAMETERS index='tr_polys')
        OVER()
        AS (rowid, polyid)
        FROM CI.PSH_predictor
    )
    AS intersected_polys
    ON CI.PSH_predictor.rowid=intersected_polys.rowid
)
AS intermediate
JOIN CI.PSH_landsat
ON intermediate.polyid=PSH_landsat.polyid
AND intermediate.datayear=PSH_landsat.year
")
train_data <- sqlQuery(con, join_qry)
head(train_data)

distributedR_start(cluster_conf='/opt/hp/distributedR/conf/cluster.xml')
message(date(), ": Started loading dframe")
indep_data <- db2dframe("CI.PSH_predictor", pred_cols, "hack14")
message(date(), ": Finished loading dframe")

message(date(), ": Training randomForest model")
rfmodel <- hpdrandomForest(x=train_data[names(train_data) != "class"],
                           y=train_data$class, importance=TRUE,
                           nExecutor=4)
message(date(), ": finished training randomForest model")
message(date(), ": Started predicting from randomForest model")
res <- predictHPdRF(rfmodel, newdata=indep_data)
message(date(), ": finished predicting from randomForest model")
