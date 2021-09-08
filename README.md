# MusicApp Data Lake Project - Spark On AWS

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move 
their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app.

Goal of this project is to build an ETL pipeline that extracts their data from S3, 
processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 
This will allow their analytics team to consume data from these fact-dimensional tables, 
to continue finding insights in what songs their users are listening to.

Deliverable: a spark job file which can be run in spark cluster to perform all the aboved tasks.


## 1 About the job files

Files in this repo:

	etl-EMR.py	--spark job file running on AWS EMR(read from S3 and write to S3)	
	etl-local.py	--spark job file running on local(read local file and write to local dir)
	Readme.md	-- project introduction



## 2 Original Data
All the files are stored in S3, below are the bucket and file information

	� Song data:�s3://udacity-dend/song_data
	� Log data:�s3://udacity-dend/log_data

There are 2 types of data: song files, and log files

1.1 Song metadata
Song dataset a from 'Million Song Dataset', here is the link: [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).
File 
Each file is a JSON format data about one song, below is a example:

		{
		"num_songs": 1, 
		"artist_id": "ARJIE2Y1187B994AB7", 
		"artist_latitude": null, 
		"artist_longitude": null, 
		"artist_location": "", 
		"artist_name": "Line Renaud", 
		"song_id": "SOUPIRU12A6D4FA1E1", 
		"title": "Der Kleine Dompfaff", 
		"duration": 152.92036,
		 "year": 0
		}

1.2 Music APP log files
These log files describes users activitiy information: who from where at what time click which song on which page using which software.

Below is a example of log JSON file


		{
		"artist": "Survivor",
		"auth": "Logged In",
		"firstName": "Jayden",
		"gender": "M",
		"itemInSession": 0,
		"lastName": "Fox",
		"length": 245.36771,
		"level": "free",
		"location": "New Orleans-Metairie, LA",
		"method": "PUT",
		"page": "NextSong",
		"registration": 1541033612796.0,
		"sessionId": 100,
		"song": "Eye Of The Tiger",
		"status": 200,
		"ts": 1541110994796,
		"userAgent": "\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
		"userId": "101"
		}


## 3 Relations of Target Tables 

Spark job will create 5 tables including 1 fact table (songplays), and 4 dimensional tables (songs, artists, time and users).

Below is the ERD shows their relations.


![MusicApp, the New Schema](./assets/images/NewSchema.png)


Note: lines between tables are for reference only, there is no enforce foreign key relationship between them.


## 4 Main Spark Job

Main spark job file is the "ETL.py", this file contain two functions.

First function: process_song_data


- load song file from S3 into spark dataFrame
- select "song_id","title","artist_id","year","duration" as a new dataframe, drop duplicates, 
write it into songs table S3 as parquet file partition by "year","artist_id".
- select "artist_id","artist_name","artist_location","artist_latitude","artist_longitude", drop duplicates,
write it into artists table in S3 in parquet format 


Second function: process_log_data

- load logs file from S3 into spark dataFrame
- select "userId","firstName","lastName","gender","level" as a new dataframe, drop duplicates, 
write it into Users table S3 as parquet file.
- select timestamp of logs file, convert it into "hour","day","week","month","year","dow", 
drop duplicates, and write it into Time table in S3 partition by "year","month".
- for the fact table "songplays", use sparkSQL to join table logs with songs and artists.


'--------------------This is the end.-----------------------'
