from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from dateutil.parser import parse
from operator import add
import pyspark
import sys
import requests


# takes a triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#((pickup_date), value) 
def mapper(x,value):
	# extract the year and the month and map them to 2
	return(x[0],value)

conf = SparkConf()
conf.setAppName("Taxi_vs_Uber")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# Create an sql context so that we can query data files in sql like syntax
sqlContext = SQLContext (sc)
# read the csv file

#---------------preping the weather data
#reading 
weatherFile = sc.textFile("file:///app/nyc_cleanData_2015-16.csv")
# This will map each day to it's weather state
weather_temp = weatherFile.map(lambda line: line.split(",")).map(lambda x : (x[0],x[2]))
# since it will be used multiple times
weather_temp.cache()

#------------Uber-------------------
uberFile = sc.textFile("file:///app/uber_combined.csv")

# for each line in the Uber file, this line will map the pickup_date to tuple, check the mapper function for details about the tuple.
#------ Note: we're mapping the pickup date to 2 because we know that the data we're parsing is like only 50% of all uber rides in NYC.
uber_temp = uberFile.map(lambda line: line.split(",")).map(lambda x : mapper(x,2))

uber_temp_2015= uber_temp.filter(lambda x: "2015" in str(x[0]))
uber_temp_2014= uber_temp.filter(lambda x: "2014" in str(x[0]))



#add the rides in the same day
uber_temp_2015= uber_temp_2015.reduceByKey(add)
uber_temp_2014= uber_temp_2014.reduceByKey(add)

# join with weather rdd, for each entry this will produce a triplet in the following format
# (date,(# rides, weather)) 
uber_temp_2015 = uber_temp_2015.join(weather_temp)
uber_temp_2014 = uber_temp_2014.join(weather_temp)


# remap each entry in rdd, this will produce a tuple in the following format (weather, # rides)
uber_temp_2015 = uber_temp_2015.map(lambda x: (x[1][1],x[1][0]))
uber_temp_2014 = uber_temp_2014.map(lambda x: (x[1][1],x[1][0]))


# count the number of Rainy,Snowy and Normal weather days
NumberOfdaysForEachWeatherStatusUber2015 = uber_temp_2015.countByKey()
NumberOfdaysForEachWeatherStatusUber2014 = uber_temp_2014.countByKey()

# broadcast 
sc.broadcast(NumberOfdaysForEachWeatherStatusUber2015)
sc.broadcast(NumberOfdaysForEachWeatherStatusUber2014)

#add the rides that share the same weather status. so basically this will add the rides in Rainy weather togaether, Normal weather togaether and Snowy weather togather.
# at the end each rdd show have only three entries
# (Rain,# rides taken in raniy weather across the entire peroid)
# (Snow,# rides taken in Snowy weather across the entire peroid)
# (Normal,# rides taken in Normal weather across the entire peroid)
uber_temp_2015= uber_temp_2015.reduceByKey(add)
uber_temp_2014= uber_temp_2014.reduceByKey(add)

# calculate the average Number of rides taken in each weather state
uber_temp_2015=uber_temp_2015.map(lambda x: (x[0],x[1]/NumberOfdaysForEachWeatherStatusUber2015[x[0]]))
uber_temp_2014=uber_temp_2014.map(lambda x: (x[0],x[1]/NumberOfdaysForEachWeatherStatusUber2014[x[0]]))


print("Number of uber rides weather status 2015")
print(sorted(uber_temp_2015.collect()))

uber_temp_2015.saveAsTextFile("file:///app/Uber_weather2015_processed")

print("Number of uber rides weather status 2014")
print(sorted(uber_temp_2014.collect()))

uber_temp_2014.saveAsTextFile("file:///app/Uber_weather2014_processed")


#------------taxi-------------------

taxiFile = sc.textFile("file:///app/taxi_combined.csv")

# for each line in the Uber file, this line will map the pickup_date to tuple, check the mapper function for details about the tuple.
taxi_temp = taxiFile.map(lambda line: line.split(",")).map(lambda x : mapper(x,1))

taxi_temp_2015= taxi_temp.filter(lambda x: "2015" in str(x[0]))
taxi_temp_2014= taxi_temp.filter(lambda x: "2014" in str(x[0]))


#add the rides in the same day
taxi_temp_2015= taxi_temp_2015.reduceByKey(add)
taxi_temp_2014= taxi_temp_2014.reduceByKey(add)


# join with weather rdd, for each entry this will produce a triplet in the following format
# (date,(# rides, weather)) 
taxi_temp_2015 = taxi_temp_2015.join(weather_temp)
taxi_temp_2014 = taxi_temp_2014.join(weather_temp)



# remap each entry in rdd, this will produce a tuple in the following format (weather, # rides)
taxi_temp_2015 = taxi_temp_2015.map(lambda x: (x[1][1],x[1][0]))
taxi_temp_2014 = taxi_temp_2014.map(lambda x: (x[1][1],x[1][0]))


# count the number of Rainy,Snowy and Normal weather days
NumberOfdaysForEachWeatherStatusTaxi2015 = taxi_temp_2015.countByKey()
NumberOfdaysForEachWeatherStatusTaxi2014 = taxi_temp_2014.countByKey()


# broadcast 
sc.broadcast(NumberOfdaysForEachWeatherStatusTaxi2015)
sc.broadcast(NumberOfdaysForEachWeatherStatusTaxi2014)


#add the rides that share the same weather status. so basically this will add the rides in Rainy weather togaether, Normal weather togaether and Snowy weather togather.
# at the end each rdd show have only three entries
# (Rain,# rides taken in raniy weather across the entire peroid)
# (Snow,# rides taken in Snowy weather across the entire peroid)
# (Normal,# rides taken in Normal weather across the entire peroid)
taxi_temp_2015= taxi_temp_2015.reduceByKey(add)
taxi_temp_2014= taxi_temp_2014.reduceByKey(add)


# calculate the average Number of rides taken in each weather state
taxi_temp_2015=taxi_temp_2015.map(lambda x: (x[0],x[1]/NumberOfdaysForEachWeatherStatusTaxi2015[x[0]]))
taxi_temp_2014=taxi_temp_2014.map(lambda x: (x[0],x[1]/NumberOfdaysForEachWeatherStatusTaxi2014[x[0]]))

#print
print("Number of taxi rides weather status 2015")
print(sorted(taxi_temp_2015.collect()))

taxi_temp_2015.saveAsTextFile("file:///app/Taxi_weather2015_processed")

print("Number of taxi rides weather status 2014")
print(sorted(taxi_temp_2014.collect()))

taxi_temp_2014.saveAsTextFile("file:///app/Taxi_weather2014_processed")
