import configparser
import datetime as dt
import re
import os
from pyspark.sql import Column
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf, col, round, lit, split
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,\
								  date_format
from pyspark.sql.types import TimestampType, StructType, StructField,\
							  StringType, BooleanType, IntegerType, \
							  FloatType, DoubleType, DecimalType


#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


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
	list=[]
	for header in headers:
		head=re.sub('[()]','_',header)
		head=head.rstrip('_')
		list.append(head)
	return list

def process_all_data(spark, input_data, output_data):
	'''
	Description: 
			imports S3 data into variable song_data
			processes song_data into tables songs and artist,
			then saves them to a new S3 path

	Arguments:
			spark : spark session
			input_data: contains S3 path where initial data is held
			output_data: new path to S3 which will hold formatted data
	'''
	acc_schema = StructType([
		StructField('ID' ,                     			StringType() , 		False),
		StructField('Source',                  		StringType() , 		False),
		StructField('TMC',                    			StringType(),		True),
		StructField('Severity',                		IntegerType() ,  		False),  
		StructField('Start_Time',              		TimestampType(), 	False), 
		StructField('End_Time',             		TimestampType(), 	False),
		StructField('Start_Lat',               		DecimalType(8,4),    	False), 
		StructField('Start_Lng',               		DecimalType(8,4),  	False), 
		StructField('End_Lat' ,                		DecimalType(8,4),	True),
		StructField('End_Lng' ,                		DecimalType(8,4),	True),
		StructField('Distance(mi)' ,           		DecimalType(8,2) ,    	False),
		StructField('Description',             		StringType() ,   		False),
		StructField('Number',                  		IntegerType(),		True),
		StructField('Street',                  			StringType(),		True),
		StructField('Side' ,                   			StringType(),		True),
		StructField('City',                    			StringType(),		True),
		StructField('County',                  		StringType(),		True),
		StructField('State',                   			StringType(),		True),
		StructField('Zipcode',                 		StringType(),		True),
		StructField('Country' ,                		StringType(),		True),
		StructField('Timezone',                		StringType(),		True),
		StructField('Airport_Code',            	 	StringType(),		True),
		StructField('Weather_Timestamp',        TimestampType(), 	True),
		StructField('Temperature(F)' ,         	DoubleType(),		True),
		StructField('Wind_Chill(F)' ,          		DoubleType(),		True),
		StructField("Humidity(%)" ,          		DoubleType(),		True),
		StructField("Pressure(in)",          		DoubleType(),		True),
		StructField('Visibility(mi)',         		DoubleType(),		True),
		StructField('Wind_Direction' ,         	StringType(),		True),
		StructField('Wind_Speed(mph)' ,        	DoubleType(),		True),
		StructField("Precipitation(in)",     		DoubleType(),		True),
		StructField('Weather_Condition',     	StringType(),		True),
		StructField('Amenity',                 		BooleanType(),   	False),
		StructField('Bump',                   		BooleanType() ,  	False),
		StructField('Crossing',                		BooleanType() ,  	False),
		StructField('Give_Way',                		BooleanType() ,  	False),
		StructField('Junction',                		BooleanType() ,  	False),
		StructField('No_Exit',                 		BooleanType() ,  	False),
		StructField('Railway',                 		BooleanType() ,  	False),
		StructField('Roundabout',              		BooleanType() ,  	False),
		StructField('Station',                 		BooleanType() ,  	False),
		StructField('Stop',                   			BooleanType() ,  	False),
		StructField('Traffic_Calming',         	BooleanType() ,  	False),
		StructField('Traffic_Signal',         		BooleanType() ,  	False),
		StructField('Turning_Loop',            	BooleanType() ,  	False),
		StructField('Sunrise_Sunset',          	StringType(),		True),
		StructField('Civil_Twilight',         		StringType(),		True),
		StructField('Nautical_Twilight',       	StringType(),		True),
		StructField('Astronomical_Twilight',    StringType(),		True)
	])
	
	wthr_schema = StructType([	
		StructField('EventId',                 StringType(),     False),
		StructField('Type',                    StringType() ,    False),
		StructField('Severity',                StringType(),	 True),
		StructField('StartTime(UTC)',          TimestampType(),  False),
		StructField('EndTime(UTC)',            TimestampType() , False),
		StructField('TimeZone' ,               StringType() ,    False),
		StructField('AirportCode',             StringType(),	 True),
		StructField('LocationLat',             DoubleType(),	  	 True),
		StructField('LocationLng',             DoubleType(),	  	 True),
		StructField('City',                    StringType(),	 True),
		StructField('County',                  StringType(),	 True),
		StructField('State',                   StringType(),	 True),
		StructField('ZipCode',                 StringType(),	 True)
	])
	#get data
	accident_data = input_data + 'US_Accidents_Dec19.csv' 
	weather_data = input_data + 'US_WeatherEvents_2016-2019.csv'

	# read in CSV data file
	acc_df = spark.read.csv(accident_data, header=True, schema = acc_schema)
	print('accident_data loaded')

	wthr_df = spark.read.csv(weather_data, header=True, schema = wthr_schema)
	print('weather_data loaded')
	
	#Clean df headers
	acc_names=update_header(acc_df.columns)
	print('acc_df headers cleaned')
	
	wthr_names=update_header(wthr_df.columns)
	print('wthr_df headers cleaned')
	
	#Replace headers on old df with new
	acc_df = acc_df.toDF(*acc_names)
	acc_df=acc_df.withColumnRenamed('Humidity_%', 'Humidity')
	print('acc_df headers updated')    
	
	wthr_df = wthr_df.toDF(*wthr_names)
	print('acc_df headers updated')
	
	#Fill missing data for description
	acc_df=acc_df.fillna('no data', subset= 'Description')
	
	#Create Date and Time columns for both DFs
	acc_df=acc_df.withColumn('Date', split(col('Start_Time'), ' ').getItem(0))
	acc_df=acc_df.withColumn('Time', split(col('Start_Time'), ' ').getItem(1))
	wthr_df=wthr_df.withColumn('Date', split(col('StartTime_UTC'), ' ').getItem(0))
	wthr_df=wthr_df.withColumn('Time', split(col('StartTime_UTC'), ' ').getItem(1))
	
	
	#Create temp accident table 
	acc_df.createOrReplaceTempView("acc_table")
	print('acc_table sql table created')
	
	#Create temp weather table 
	wthr_df.createOrReplaceTempView('wthr_table')
	print('wthr_table sql table created')
	
	

	
	accident= spark.sql("""
	SELECT
		id,
		source,
		tmc,
		severity,
		start_time AS starttime,
		date,
		time,
		distance_mi AS distance,
		description,
		cast(concat('W-', row_number() over (order by "id")) as string) AS weather_id,
		cast(concat('P-', row_number() over (order by "id")) as string) AS area_id
	FROM acc_table
	""")

	print('accident table created')
	
	# write accident table to parquet files
	accident.write.mode('overwrite').parquet(output_data + 'accident')
	print('accident file saved')
	
	location= spark.sql("""
	SELECT 
		DISTINCT(id) AS loc_id,
		street,
		side,
		city,
		county,
		state,
		zipcode,
		country,
		start_lat     AS lat,
		start_lng     AS long                   
	FROM acc_table
	""")
	print('location table created')

	# write location table to parquet files
	location.write.mode('overwrite').parquet(output_data + "location")
	print('location file saved')

	# extract columns to create artists table
	time = spark.sql("""
	SELECT 
		start_time, 
		EXTRACT(hour      FROM start_time)    	AS hour, 
		EXTRACT(day       FROM start_time)    	AS day, 
		EXTRACT(week      FROM start_time)    	AS week, 
		EXTRACT(month     FROM start_time)    	AS month, 
		EXTRACT(year      FROM start_time)    	AS year, 
		EXTRACT(dayofweek FROM start_time)      AS weekday
	FROM acc_table
		""")
	print('time table created')

	# write time table to parquet files
	time.write.mode('overwrite').parquet(output_data + 'time')
	print('time file saved')

	weather = spark.sql("""
	SELECT  
		cast(concat('W-', row_number() over (order by "ac.id")) as string) AS weather_id,
		ac.temperature_f    AS temp,
		wt.type,
		wt.severity,
		ac.wind_chill_f     AS wind_chill,
		ac.humidity,
		ac.pressure_in      AS pressure,
		ac.visibility_mi    AS visibility,
		ac.wind_direction,
		ac.wind_speed_mph   AS wind_speed,
		ac.precipitation_in AS precipitation,
		wt.airportcode,
		wt.date,
		wt.zipcode
	FROM acc_table ac
	LEFT JOIN wthr_table wt
	ON ac.date = wt.date 
	AND ac.zipcode = wt.zipcode
	""")

	print('weather table created')

	# write weather table to parquet files
	weather.write.mode('overwrite').parquet(output_data + 'weather')
	print('weather file saved')

	area_poi = spark.sql("""
	SELECT 
	    cast(concat('P-', row_number() over (order by "id")) as string) AS poi_id,
		amenity,
		bump,
		crossing,
		give_way,
		junction,
		no_exit,
		railway,
		roundabout,
		station,
		stop,
		traffic_calming       AS trfc_calm,
		traffic_signal        AS trfc_sig,
		turning_loop          AS turn_lp,
		sunrise_sunset        AS sunrise_set,
		civil_twilight        AS civ_twi,
		nautical_twilight     AS naut_twi,
		astronomical_twilight AS astro_twi
	FROM acc_table
	""")
	print('area table created')
	
	# write weather table to parquet files
	area_poi.write.mode('overwrite').parquet(output_data + 'area_poi')
	print('area_poi file saved')

    

def main():
	'''
	Description: Arguments to be used in functions above
	'''
	spark = create_spark_session()
	input_data = "s3a://garcia-capstone/input/"
	output_data = "s3a://garcia-capstone/output/"

	process_all_data(spark, input_data, output_data)    

    

if __name__ == "__main__":
    main()


    