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

##In general, we usually start by analyzing data in Spark with dplyr, 
##followed by sampling rows and selecting a subset of the available columns. 
##Plot Example##
select(cars, hp, mpg) %>%
  sample_n(100) %>%
  collect() %>%
  plot()
## For dplyr reference go to https://r4ds.had.co.nz/transform.html

#2.4.3: MODELING
#Example from a linear model to approximate the relationship between fuel efficiency and horsepower:
model <- ml_linear_regression(cars, mpg ~ hp)
model
# With the values created by the model we can predict future values
model <- ml_linear_regression(cars, mpg ~ hp)


# Predição de 10 valores acima das 250 amostras capturadas
model %>%
  ml_predict(copy_to(sc, data.frame(hp = 250 + 10 * 1:10))) %>%
  transmute(hp = hp, mpg = prediction) %>%
  full_join(select(cars, hp, mpg)) %>%
  collect() %>%
  plot()

#2.4.4 DATA
#In the beginning we copied CARS into Spark dataset.
#Now with spark_write_csv, we will copy the CARS dataset to a local csv file called cars.csv

spark_write_csv(cars, "cars1.csv")

#In practice, we would read an existing dataset from a distributed storage system like HDFS, but we can also read back from the local file system:
cars <- spark_read_csv(sc, "cars.csv")


#2.4.5 Extensions ( Not Covered here)
#2.4.6 Distributed R (Not Covered here)
#2.4.7 Streaming: While processing large static datasets is the most typical use case for Spark, processing dynamic datasets in real time is also possible and, for some applications, a requirement. You




#To try out streaming, let’s first create an input/ folder with some data that we 
#will use as the input for this stream:
dir.create("input")
write.csv(mtcars, "input/cars_1.csv", row.names = F)

#Then, we define a stream that processes incoming data from the input/ folder, 
#performs a custom transformation in R, and pushes the output into an output/ folder:

stream <- stream_read_csv(sc, "input/") %>%
  select(mpg, cyl, disp) %>%
  stream_write_csv("output/")

dir("output", pattern = ".csv")

# DID NOT FINISH THIS SESSION

#2.4.8 - LOGS
spark_log(sc)

#2.5 Disconecting
spark_disconnect(sc)
spark_disconnect_all()
# https://spark.rstudio.com/

  
#3 Analysis
library(sparklyr)
library(dplyr)
library(ggplot2)
library(rmarkdown)
library(corrr)
library(dbplot)
sc <- spark_connect(master = "local", version = "3.0.1")
#3.3 Import
#copy_to - Is used to transfer just small tables into spark
#For large datasets, some other specialized tools should be used.
cars <- copy_to(sc, mtcars)

#3.3 Wrangle
summarize_all(cars, mean)

#Function shows the query used únder the hood
summarize_all(cars, mean) %>%
  show_query()

# Another example that groups the cars dataset by transmission type:
cars %>%
  mutate(transmission = ifelse(am == 0, "automatic", "manual")) %>%
  group_by(transmission) %>%
  summarise_all(mean)


#3.3.1 Built-in functions
summarise(cars, mpg_percentile = percentile(mpg, 0.25))

summarise(cars, mpg_percentile = percentile(mpg, 0.25)) %>%
  show_query()

summarise(cars, mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75)))


summarise(cars, mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75))) %>%
  mutate(mpg_percentile = explode(mpg_percentile))


#3.3.2 Correlations


ml_corr(cars)

library(corrr)
correlate(cars, use = "pairwise.complete.obs", method = "pearson") 


correlate(cars, use = "pairwise.complete.obs", method = "pearson") %>%
  shave() %>%
  rplot()





#######################################################################
#################### Reading a CSV File
##sc <- spark_connect(master = "local", version = "3.0.1")
##diretorio = 'C:/qsa_cnpj/bd_cnpj_tratados'
##setwd(diretorio)
##spark_read_csv(sc,'C:/qsa_cnpj/bd_cnpj_tratados/cnpj_dados_socios_pj.csv', delimiter = '#')




