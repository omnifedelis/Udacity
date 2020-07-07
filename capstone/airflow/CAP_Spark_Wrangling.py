import datetime as dt
import re
import os
from pyspark.sql import Column
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf, col, round, lit, split, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,\
								  date_format
from pyspark.sql.types import TimestampType, StructType, StructField,\
							  StringType, BooleanType, IntegerType, \
							  FloatType, DoubleType, DecimalType





def create_spark_session():	
	'''
	Initializes spark session and aws hadoop environment
	'''
	spark = SparkSession \
	    .builder \
	    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
	    .getOrCreate()
	return spark

def update_header(headers):
        '''
	Updates column headers by removing parenthesis 
	and replacing with underscore
	'''
	list=[]
	for header in headers:
		head=re.sub('[()]','_',header)
		head=head.rstrip('_')
		list.append(head)
	return list

def process_all_data(spark, input_data, output_data):

	'''
	Description: 
			imports S3 data into variables
			processes accident and weather data into tables,
			then saves them to S3 output_data path as parquet files

	Arguments:
			spark : spark session
			input_data: contains S3 path where initial data is held
			output_data: new path to S3 which will hold formatted data
	'''

	#get data
	accident_data = input_data + 'TrafficEvents_Aug16_Dec19_Publish.csv' 
	weather_data = input_data + 'WeatherEvents_Aug16_Dec19_Publish.csv'

	# read in CSV data file
	acc_df = spark.read.csv(accident_data, header=True)
	print('accident_data loaded')

	wthr_df = spark.read.csv(weather_data, header=True)
	print('weather_data loaded')

	#Clean df headers
	acc_names = update_header(acc_df.columns)
	print('acc_df headers cleaned')

	wthr_names = update_header(wthr_df.columns)
	print('wthr_df headers cleaned')

	#Replace headers on old df with new
	acc_df = acc_df.toDF(*acc_names)
	print('acc_df headers updated')    

	wthr_df = wthr_df.toDF(*wthr_names)
	print('acc_df headers updated')

	#Fill missing data for description
	acc_df=acc_df.fillna('no data', subset= 'Description')

	#Create Date and Time columns for both DFs
	acc_df  = acc_df.withColumn('Date', split(col('StartTime_UTC'), ' ').getItem(0))
	acc_df  = acc_df.withColumn('Time', split(col('StartTime_UTC'), ' ').getItem(1))
	wthr_df = wthr_df.withColumn('Date', split(col('StartTime_UTC'), ' ').getItem(0))
	wthr_df = wthr_df.withColumn('Time', split(col('StartTime_UTC'), ' ').getItem(1))

	#Cast correct data formats for specific columns
	acc_df  = acc_df.withColumn('StartTime_UTC', to_timestamp('StartTime_UTC',"yyyy-MM-dd HH:mm:ss"))
	acc_df  = acc_df.withColumn('EndTime_UTC', to_timestamp('EndTime_UTC',"yyyy-MM-dd HH:mm:ss"))
	acc_df  = acc_df.withColumn("LocationLat", acc_df["LocationLat"].cast(DecimalType(8,4)))
	acc_df  = acc_df.withColumn("LocationLng", acc_df["LocationLng"].cast(DecimalType(8,4)))
	acc_df  = acc_df.withColumn("Distance_mi", acc_df["Distance_mi"].cast(DecimalType(8,2)))
	wthr_df = wthr_df.withColumn('StartTime_UTC', to_timestamp('StartTime_UTC',"yyyy-MM-dd HH:mm:ss"))
	wthr_df = wthr_df.withColumn('EndTime_UTC', to_timestamp('EndTime_UTC',"yyyy-MM-dd HH:mm:ss"))
	wthr_df = wthr_df.withColumn("LocationLat", wthr_df["LocationLat"].cast(DecimalType(8,4)))
	wthr_df = wthr_df.withColumn("LocationLng", wthr_df["LocationLng"].cast(DecimalType(8,4)))

	#Create temp accident table 
	acc_df.createOrReplaceTempView("acc_table")
	print('acc_table sql table created')
	
	#Create temp weather table 
	wthr_df.createOrReplaceTempView('wthr_table')
	print('wthr_table sql table created')
	
	

	#Create accident table
	accident = spark.sql("""
	SELECT
		eventid       AS accident_id,
		type,
		severity,
		tmc,
		description,
		starttime_utc AS starttime,
		date,
		time,
		distance_mi AS distance
	FROM acc_table
	""")

	print('accident table created')

	# Write accident table to parquet files
	accident.write.mode('overwrite').parquet(output_data + 'accident')
	print('accident file saved')
	
	# Create location table
	location = spark.sql("""
	SELECT 
		DISTINCT(eventid) AS loc_id,
		number,
		street,
		side,
		city,
		county,
		state,
		zipcode,
		locationlat       AS lat,
		locationlng       AS long                   
	FROM acc_table
	""")
	print('location table created')

	# Write location table to parquet files
	location.write.mode('overwrite').parquet(output_data + "location")
	print('location file saved')

	# Create time table
	time = spark.sql("""
	SELECT starttime_utc                         AS starttime, 
		EXTRACT(hour      FROM starttime_utc)    AS hour, 
		EXTRACT(day       FROM starttime_utc)    AS day, 
		EXTRACT(week      FROM starttime_utc)    AS week, 
		EXTRACT(month     FROM starttime_utc)    AS month, 
		EXTRACT(year      FROM starttime_utc)    AS year, 
		EXTRACT(dayofweek FROM starttime_utc)    AS weekday
	FROM acc_table
	""")

	print('time table created')

	# Write time table to parquet files
	time.write.mode('overwrite').parquet(output_data + 'time')
	print('time file saved')
    
	#Create weather table
	weather = spark.sql("""
	SELECT  
		eventid          AS weather_id,
		type,
		severity,
		starttime_utc    AS weather_start,
		date,
		time,
		airportcode,
		city,
		state,
		zipcode
	FROM wthr_table
	""")

	print('weather table created')

	# Write weather table to parquet files
	weather.write.mode('overwrite').parquet(output_data + 'weather')
	print('weather file saved')
	
	print('All tables have been created and saved')


    

def main():
	'''
	Description: Arguments to be used in functions above
	'''
	spark       = create_spark_session()
	input_data  = "s3a://garcia-capstone/input/"
	output_data = "s3a://garcia-capstone/output/"

	process_all_data(spark, input_data, output_data)    

    

if __name__ == "__main__":
    main()


    
