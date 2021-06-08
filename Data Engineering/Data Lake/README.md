



# <center> Sparkify's Data Lake Using Amazon's AWS Service </center>

## Intro
------
The project below is based on a startup company called Sparkify. Sparkify's user base has grown to a point where it has been
decided that their current processes and data need to be relocated to the cloud. The data resides in S3, in a directory of 
JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. As their data engineer 
I have been tasked with the duty of building a ETL pipeline that will extract the data stored in S3, process the data with Spark and 
loads the data back into S3 as a set of dimensional tables for their analytics team. This will allow them to continue finding insights on what songs their users are listening to. Once the database and ETL pipeline are completed, I should be able to run queries given to me by the analytics 
team from Sparkify and compare my results.



## Purpose
------

The purpose of this project is to apply what I have learned about Spark and Data Lakes using Amazon's AWS EMR service.
I am to build an ETL pipeline for a data lake hosted on S3. I will need to move data from two S3 buckets, process the data using Spark 
and load the data back into another S3 bucket.


## Schema
------
We will be using a design similar to the star schema design shown below. This design allows for the use of individual tables to perform 
queries in specific fields or the use of JOIN statements to combine tables across the database for collective analytical
interest.


<div align="center">
  <img width="800" height="500" src="Complete_Schema.png">
</div>



### Staging Events Table

**staging_events**- dataset of log files in JSON format generated by an event simulator
*event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId*

### Staging Songs Table

**staging_songs**- dataset is a subset of real data from the Million Song Dataset
*songs_id, num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year*



### Fact Table

**songplays** - records in log data associated with song plays i.e. records with page NextSong  
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*  

### Dimension Tables

**users** - users in the app  
*user_id, first_name, last_name, gender, level*  

**songs** - songs in music database  
*song_id, title, artist_id, year, duration*  

**artists** - artists in music database  
*artist_id, name, location, latitude, longitude*  

**time** - timestamps of records in songplays broken down into specific units  
*start_time, hour, day, week, month, year, weekday*




## Python Scripts And Descriptions
-------
We will be using the scripts below to gather, transform, and process the data from the S3 bucket directories provided to us 
which are found in the dl.cfg file below.

**dl.cfg**-Contains AWS access key id and secret key id needed for the AWS session.  
    
**etl.py**- Contains scripts for processing song_data, log_data, initiating a spark session, and running those defined functions needed for pulling and inserting S3 data.  

**Complete_Schema.png**- Holds the Schema image used in the README.md file  

**README.md**-provides discussion on the project.  


## Order of Scripts

- fill in the dl.cfg with appropiate information   
- run the etl.py script