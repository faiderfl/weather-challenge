
from pyspark.sql import SparkSession
from pyspark import SparkContext 
from pyspark.sql import SQLContext 
from pyspark.sql.window import Window
from pyspark.sql.functions import lag,lead,to_timestamp,col, when, mean, split, explode, expr,row_number, sum, max, min,avg, countDistinct, substring,dense_rank
from pyspark.sql.types import DoubleType, LongType, _infer_schema
from pyspark.sql.types import StructField, StructType, IntegerType, StringType,DoubleType,DecimalType

spark = SparkSession.builder.appName("Weather-Analysis").master("local[*]").getOrCreate()


#Step 1 - Setting Up the Data
#1. Load the global weather data into your big data technology of choice.
weather_data = spark.read.options(header='True').options(sep=',').options(infer_schema=True).csv("data/2019/*.csv.gz")
weather_data.printSchema()



#2. Join the stationlist.csv with the countrylist.csv to get the full country name for each station number.

country_data = spark.read.options(header='True').options(sep=',').options(infer_schema=True).csv("countrylist.csv")
station_data = spark.read.options(header='True').options(sep=',').options(infer_schema=True).csv("stationlist.csv")
country_stations= station_data.join(country_data, on=['COUNTRY_ABBR'],how='left').na.fill({'COUNTRY_FULL': 'N/A'})



#3.Join the global weather data with the full country names by station number.
weather_stations_country= weather_data.join(country_stations, on=weather_data['STN---']==country_stations['STN_NO'], how='left')
weather_stations_country.show(5,False)
weather_stations_country.cache()

#Step 2 - Questions
#1. Which country had the hottest average mean temperature over the year?
hottest_country= weather_stations_country.groupBy('COUNTRY_FULL').agg(avg('temp')).orderBy('avg(temp)', ascending=False)
hottest_country.show(1, False)

#2. Which country had the most consecutive days of tornadoes/funnel cloud formations?
wsc= weather_stations_country.withColumn('tornado/funnel',substring(weather_stations_country['FRSHTT'], -1, 1)).where('FRSHTT')
wsc_consecutive= wsc.withColumn("lag", lag('tornado/funnel').over(Window.partitionBy('COUNTRY_FULL').orderBy('YEARMODA')))
#wsc_consecutive = wsc_consecutive.withColumn("new_secuence", when(col('tornado/funnel')=='1' & col('lag')=='1',0).otherwise(1))
#wsc_consecutive.wsc_consecutive.show(1000,False)



#3. Which country had the second highest average mean wind speed over the year?
wsc_without_default_wind_speed= weather_stations_country.where('WDSP !=999.9')
highest_avg_wind = wsc_without_default_wind_speed.groupBy('COUNTRY_FULL').agg(avg('WDSP')).withColumnRenamed('avg(WDSP)','avg_WDSP').orderBy('avg_WDSP', ascending=False)
highest_avg_wind = highest_avg_wind.withColumn("dense_rank", dense_rank().over(Window.partitionBy().orderBy(col("avg_WDSP").desc())))
highest_avg_wind.where('dense_rank==2').show(1,False)