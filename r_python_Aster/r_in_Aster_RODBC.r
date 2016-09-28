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

my_schema = "<fill in>"

library(RODBC)

asterCluster <- odbcConnect(dsn = "<fill in>")

# set your working directory to the location of R files
setwd("<fill in>")
getwd()


try(system(sprintf("mv create_df_one.R create_df_one_%s.R", my_schema), intern = TRUE))
try(system(sprintf("mv manipulate_df_one.R manipulate_df_one_%s.R", my_schema), intern = TRUE))
try(system(sprintf("mv aggregate_df_two.R aggregate_df_two_%s.R", my_schema), intern = TRUE))


#================================================================================
#================================================================================
# first query. create df_one
#================================================================================
#================================================================================

# install the file in Aster
sqlQuery(asterCluster,  sprintf("uninstall file 'create_df_one_%s.R'", my_schema))
sqlQuery(asterCluster,  sprintf("install file 'create_df_one_%s.R'", my_schema))

# query using the r file create_df_one
query1 = sprintf(
"SELECT * 
FROM stream(
ON (select 1)
SCRIPT('Rexec create_df_one_%s.R')
OUTPUTS('row_id int',
        'letter varchar',
        'y numeric')
);
", my_schema)

# send query and see result
sqlQuery(asterCluster, query1)

## we need this table for next exercise, lets create a table into Aster with JODBC
# create a data.frame
sqlQuery(asterCluster,sprintf("drop table if exists %s.df_one;", my_schema) )

sqlQuery(asterCluster,sprintf( "
create table %s.df_one distribute by hash (row_id) as
SELECT * 
FROM stream(
ON (select 1)
SCRIPT('Rexec create_df_one_%s.R')
OUTPUTS('row_id int',
        'letter varchar',
        'y numeric')
);
", my_schema, my_schema) 
)

# check if the table is there
tables <- sqlTables(asterCluster)
tables[tables['TABLE_SCHEM'] == my_schema,]

#================================================================================
#================================================================================
# second query. manipulate an existing table using r
#================================================================================
#================================================================================

# install the file in Aster
sqlQuery(asterCluster,  sprintf("uninstall file 'manipulate_df_one_%s.R'", my_schema))
sqlQuery(asterCluster,  sprintf("install file 'manipulate_df_one_%s.R'", my_schema))

query2 = 
sprintf("create table %s.df_two distribute by hash (row_id) as
 SELECT *
 FROM stream(
 ON %s.df_one
 SCRIPT('Rexec manipulate_df_one_%s.R')
 OUTPUTS('row_id int',   
        'letter varchar',  
        'y numeric',
        'vowel bool',
        'y_positive  bool', 
        'id_mod_3 int'
        ) 
);", my_schema, my_schema,my_schema)

sqlQuery(asterCluster,sprintf("drop table if exists %s.df_two;", my_schema) )
sqlQuery(asterCluster, query2)

# check if the table is there
tables <- sqlTables(asterCluster)
tables[tables['TABLE_SCHEM'] == my_schema,]

# get content
sqlQuery(asterCluster,sprintf("select * from %s.df_two limit 10;", my_schema) )

#================================================================================
#================================================================================
# different options for running an r script in the Aster cluster
#================================================================================
#================================================================================

# install the file in Aster
sqlQuery(asterCluster, sprintf("uninstall file 'aggregate_df_two_%s.R'", my_schema))
sqlQuery(asterCluster, sprintf("install file 'aggregate_df_two_%s.R'", my_schema))


## partition by 1
query3 = 
sprintf("SELECT *
 FROM stream(
 ON %s.df_two
 PARTITION BY 1
 SCRIPT('Rexec aggregate_df_two_%s.R')
 OUTPUTS('row_id int',   
        'concat varchar',
        'y_sum numeric'
        ) 
);", my_schema, my_schema)

sqlQuery(asterCluster, query3)


## partition by vowel
query4 = 
sprintf("SELECT *
 FROM stream(
 ON %s.df_two
 PARTITION BY vowel
 SCRIPT('Rexec aggregate_df_two_%s.R')
 OUTPUTS('row_id int',   
        'concat varchar',
        'y_sum numeric'
        ) 
);", my_schema, my_schema)

sqlQuery(asterCluster, query4)

## partition by y_positive
query5 = 
sprintf("SELECT *
 FROM stream(
 ON %s.df_two
 PARTITION BY y_positive
 SCRIPT('Rexec aggregate_df_two_%s.R')
 OUTPUTS('row_id int',   
        'concat varchar',
        'y_sum numeric'
        ) 
);", my_schema, my_schema)

sqlQuery(asterCluster, query5)

## partition by id_mod_3
query6 = 
sprintf("SELECT *
 FROM stream(
 ON %s.df_two
 PARTITION BY id_mod_3
 SCRIPT('Rexec aggregate_df_two_%s.R')
 OUTPUTS('row_id int',   
        'concat varchar',
        'y_sum numeric'
        ) 
);", my_schema, my_schema)

sqlQuery(asterCluster, query6)
