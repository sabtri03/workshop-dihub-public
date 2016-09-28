#================================================================================
#================================================================================
# title : dihub demo
# version : 1.0
# date : 28-sept-2016
# author : mireia alos palop, data scientist
#================================================================================
#================================================================================

#================================================================================
#================================================================================
# setup the connection
#================================================================================
#================================================================================

my_schema = "ma186085"

library(RODBC)

asterCluster <- odbcConnect(dsn = "<fill in>")

## if you can, use TeradataAsterR 
library(TeradataAsterR)
ta.connect('<fill in>')

# set your working directory to the location of python files
setwd("<fill in>")
getwd()

# rename the files with your schema
try(system(sprintf("mv example_python.py example_python_%s.py", my_schema), intern = TRUE))
try(system(sprintf("mv cleaning_text.py cleaning_text_%s.py", my_schema), intern = TRUE))



#================================================================================
#================================================================================
# small demo to upload data into Aster
#================================================================================
#================================================================================


#### Do this if you CAN connect via TeradataAsterR

# table reviews_small
df = read.csv('reviews_small.csv')
names(df) <- tolower(names(df))
ta.create(df, table = "reviews_small", schemaName = my_schema, tableType = "fact" ,partitionKey = "id")

# table test

t <- data.frame(c('Old MacDonald had a farm', 'A green house'))
colnames(t) <- c('sentence')
ta.create(t, table = "test", schemaName = my_schema, tableType = "fact" ,partitionKey = "sentence")


#### NO ACCESS via TeradataAsterR, then copy the tables from a schema
query = 
sprintf( "create table %s.test distribute by hash (sentence) as
select *
from ma186085.test;", my_schema)

sqlQuery(asterCluster, query)

query = 
sprintf( "create table %s.reviews_small distribute by hash (id) as
select *
from ma186085.reviews_small;", my_schema)

sqlQuery(asterCluster, query)


#================================================================================
#================================================================================
# try example python script
#================================================================================
#================================================================================

sqlQuery(asterCluster, sprintf("uninstall file 'example_python_%s.py'", my_schema))
sqlQuery(asterCluster, sprintf("install file 'example_python_%s.py'", my_schema))

query = 
sprintf("
SELECT * FROM STREAM
(ON (SELECT sentence FROM %s.test)
SCRIPT ('python example_python_%s.py')
OUTPUTS ('word varchar', 'count varchar'));", my_schema, my_schema)

sqlQuery(asterCluster, query)

#================================================================================
#================================================================================
#  cleaning text columns using a python script
#================================================================================
#================================================================================

sqlQuery(asterCluster, sprintf("uninstall file 'cleaning_text_%s.py'", my_schema))
sqlQuery(asterCluster, sprintf("install file 'cleaning_text_%s.py'", my_schema))

query = 
sprintf("
SELECT * FROM STREAM
(ON (SELECT \"text\", \"id\" FROM %s.reviews_small limit 2)
SCRIPT ('python cleaning_text.py')
OUTPUTS ( 'text varchar', 'clean_text varchar'));", my_schema)

df = sqlQuery(asterCluster, query)

df
