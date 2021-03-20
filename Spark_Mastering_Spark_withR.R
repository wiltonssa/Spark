## Mastering Spark with R  BOOK
library(sparklyr)
packageVersion("sparklyr")
spark_available_versions()
spark_installed_versions()


## CONNECTING WITH SPARK IN LOCAL HOSTER ## 
sc <- spark_connect(master = "local", version = "3.0.1")

#2.4 USING SPARK
## copying the mtcars dataset into Apache Spark by using copy_to()
cars <- copy_to(sc, mtcars) 
##sc - refers to the spark connection
## the second parameter mtcars, informs  a dataset to load in R.
## Data was copied into SPARK, however we can access it via R like:
cars

##2.4.1 Web interface
spark_web(sc)
##2.4.2 Analysis
## We can either use DBI package or dplyr library
library(DBI)
dbGetQuery(sc, "SELECT count(*) FROM mtcars")

library(dplyr)
count(cars)

sc1 <- spark_connect(master = "local", version = "3.0.1")

diretorio = 'C:/qsa_cnpj/bd_cnpj_tratados'
setwd(diretorio)

cnpj <- copy_to(sc1, cnpj_dados_cadastrais_pj.csv) 

