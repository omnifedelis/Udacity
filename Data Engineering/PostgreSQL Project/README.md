



# <center>ETL Process And Information </center>

## Intro
------
The project below is based on a startup company called Sparkify. Sparkify currently has no way to query and anazlyze their 
data to show what customers are interesting in listening to. The data is stored in the JSON format and as a data engineer 
I have been tasked with the duty of building a database and ETL pipeline for analysis. Once the database and ETL pipeline 
are completed, I should be able to run queries given to me by the analytics team from Sparkify and compare my results.



## Purpose
------

The purpose of this project is to apply what I have learned about data modeling with Postgres and the ETL pipeline process while using 
python. I am to write the schema for an analytic focus and write an ETL pipeline that moves data from two local directories 
to a Postgres table using Python and SQL. The star schema is being used for the fact and dimension tables listed below.


## Schema
------
We will be using the star schema design for the database. This design allows for the use of individual tables to perform 
queries in specific fields or the use of JOIN statements to combine tables across the database for collective analytical
interest.

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
We will be using the scripts below to gather, transform, and process the data from the two directories provided to us 
which are labeled "song_data" and "log_data". The scripts must be completed in the order listed with the exception of 
the test.ipyb script which will be ran for both etl.ipynb and etl.py scripts.


**sql_queries.py**- contains all sql queries (i.e. table drop and create statements, insert statments) and is imported into the last three files above. 

**create_tables.py**- Creates sparkifydb database; Calls the sql_queries.py script for dropping and creating tables.  

**etl.ipynb**- reads and processes a single file from song_data and log_data and loads the data into the tables. 
    This notebook contains detailed instructions on the ETL process for each of the tables.  
    
**etl.py**- reads and processes files from song_data and log_data and loads them into the tables.  

**test.ipynb**-displays the first few rows of each table to let you check your database.    

**README.md**-provides discussion on your project.  



## Sample Test Queries
------


#### Search for top 5 longest songs
**%sql SELECT a.name as Band, ROUND(s.duration/60) as Minutes   
    FROM artists as a   
    JOIN songs as s ON a.artist_id = s.artist_id   
    WHERE s.duration > 400   
    ORDER BY s.duration DESC LIMIT 5;**  

5 rows affected.  

|band|minutes |
|----|----|
|Faiz Ali Faiz|10.0|
|Montserrat Caballé;Placido Domingo;Vicente Sardinero;Judith Blegen;Sherrill Milnes;Georg Solti|9.0
|Blue Rodeo|8.0|
|John Wesley|8.0|
|Trafik|7.0|

#### Search for how many users pay
**%sql SELECT count(user), level   
    From users Group by level;**  

2 rows affected.  

|count|level|
|--|--|
|74|free|  
|22|paid|  

#### Level paid vs free count by gender
**%sql SELECT gender, level, COUNT(gender)   
    FROM users   
    GROUP BY gender,users.level;**  

4 rows affected.  
 
|gender|level|count|
|---|---|---|
|F|free|40|  
|M|free|34|  
|M|paid|7|  
|F|paid|15|  
