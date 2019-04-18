# Files in My Project Repository
(1). There are 6 files and 1 data folder including two data sets in my repository.

(2). For the two .ipynb files, you can just open and run it in the workspace.
But for 3 .py files, you have to open a new terminal to execute them by typing "python filename.py".

(3). The steps for this project: 
     * sql_queries.py ---> create_tables.py ----> etl.ipynb----> etl.py ----> test.ipynb ---> readme.md
     
(4). Attention: each time you run create_table.py, you have to restart test.ipynb and etl.ipynb first to close the connection to database. 

# Purpose of this Database

(1). The analytics team at Sparkify wanted to know what songs users were listening to but did not have easy assess to the data. 
     Both datasets were on Sparkify's new music streaming app. One resided in a directory of JSON logs on user activity while the other 
     was in the directory with JSON metadata on the songs. 

(2). My goal was to create a Postgres database with tables designed to optimize queries on song play analysis. 


# Explanation of Database Schema Design and ETL Pipeline

(1). Database schema: I created a star schema optimized for queries on song analysis. 
    
     It contained 5 tables :
     
     * one fact table:songplays with columns songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agen
     
     * four dimension tables
          * users - users in the app with columns user_id, first_name, last_name, gender, level
          
          * songs - songs in music database with columns song_id, title, artist_id, year, duration
          
          * artists - artists in music database with columns artist_id, name, location, lattitude, longitude
          
          * time - timestamps of records in songplays broken down into specific units  with columns start_time, hour, day, week, month, year, weekday
          
(2). ETL Pipeline
    
    * Process Song data to insert records into songs and artists tables
    
    * Process log data to create time and users tables
    
    * Extract songs table, artists table, and original log file to create songplays table (join songs table, artists table)
    
# Example Queries and Results for Song Play Analysis

query 1: show the first five rows in songplays table 

    query:

    %sql SELECT * FROM songplays LIMIT 5;

    result: 
    songplay_id	start_time	user_id	level	song_id	artist_id	session_id	location	user_agent

    1	2018-11-29 00:00:57.796000	73	paid	None	None	954	Tampa-St. Petersburg-Clearwater, FL	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"

    2	2018-11-29 00:01:30.796000	24	paid	None	None	984	Lake Havasu City-Kingman, AZ	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"

    3	2018-11-29 00:04:01.796000	24	paid	None	None	984	Lake Havasu City-Kingman, AZ	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"

    4	2018-11-29 00:04:55.796000	73	paid	None	None	954	Tampa-St. Petersburg-Clearwater, FL	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"

    5	2018-11-29 00:07:13.796000	24	paid	None	None	984	Lake Havasu City-Kingman, AZ	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"

    query 2: show the first five rows in users table
    * query:
    %sql SELECT * FROM users LIMIT 5;

    * result:
    user_id	first_name	last_name	gender	level
    73	Jacob	Klein	M	paid
    24	Layla	Griffin	F	paid
    24	Layla	Griffin	F	paid
    73	Jacob	Klein	M	paid
    24	Layla	Griffin	F	paid

    query 3: show the first five rows in songs table
    * query:
    %sql SELECT * FROM songs LIMIT 5;

    * result:
    song_id	title	artist_id	year	duration
    SOFNOQK12AB01840FC	Kutt Free (DJ Volume Remix)	ARNNKDK1187B98BBD5	0	407.37914
    SOBAYLL12A8C138AF9	Sono andati? Fingevo di dormire	ARDR4AC1187FB371A1	0	511.16363
    SOFFKZS12AB017F194	A Higher Place (Album Version)	ARBEBBY1187B9B43DB	1994	236.17261
    SOGVQGJ12AB017F169	Ten Tonne	AR62SOJ1187FB47BB5	2005	337.68444
    SOXILUQ12A58A7C72A	Jenny Take a Ride	ARP6N5A1187B99D1A3	2004	207.43791

    query 4: What is the average duration of songs ?
    * query:
    %sql SELECT AVG(duration) AS avg_duration FROM songs;

    * result:
    avg_duration
    239.729676056338


    query 5:The proportion of male users and female users
    * query:
    # total user
    %sql SELECT COUNT(gender) FROM users;
    # number of female users
    %sql SELECT COUNT(gender) AS female_count \
    FROM users \
    WHERE gender = 'F';  
    # number of male users
    %sql SELECT COUNT(gender) AS male_count \
    FROM users \
    WHERE gender = 'M';
    # print propotion
    print(4887/6820 , 1933/6820)

    * result:
    0.7165689149560117 0.2834310850439883

# conclusion: there seems to be far more female users than male users for Spartify music streaming app. 


