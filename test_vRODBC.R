# Script to test using vRODBC on AWS cluster.
#
# Alex Zvoleff, azvoleff@conservation.org, July 2014

Sys.setenv(VERTICAINI="/home/alexz/vertica.ini")
Sys.setenv(ODBCINI="/home/alexz/odbc.ini")

library(vRODBC)

img_1990 <- sqlQuery(con, "select * from CI.pasoh_predictor where datayear=1990 order by pixelid")

head(img_1990)
