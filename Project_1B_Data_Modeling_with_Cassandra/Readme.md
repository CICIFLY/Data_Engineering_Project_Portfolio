Project: Data Modeling with Cassandra

## Objective:
A startup called Sparkify wanted to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team was particularly interested in understanding what songs users were listening to. But there was no easy way to query the data to generate the results, since the data resided in a directory of CSV files on user activity on the app.

My role was to create an Apache Cassandra database which could create queries on song play data to answer the questions. I also need to test my database by running queries given by the analytics team from Sparkify to create the results.



## Datasets
For this project, there is one dataset: event_data. The directory of CSV files partitioned by date. 
Here are examples of filepaths to two files in the dataset:

    event_data/2018-11-08-events.csv
    event_data/2018-11-09-events.csv
    link to a image of the dataset with all the columns:
    https://r766469c826649xjupyterlrhraafil.udacity-student-workspaces.com/files/images
    link to download the event data:
    https://r766469c826649xjupyterlrhraafil.udacity-student-workspaces.com/files/event_data
    

When I insert the data into Apache Cassandra tables, I generated a smaller-sized data set called event_datafile_new.csv based on event_data by extracting most related information. It contains the following columns: 

    artist
    firstName of user
    gender of user
    item number in session
    last name of user
    length of the song
    level (paid or free song)
    location of the user
    sessionId
    song title
    userId



## Import Python packages 
    import pandas as pd
    import cassandra
    import re
    import os
    import glob
    import numpy as np
    import json
    import csv



## How to run the project ?
    All the libraries above should be installed
    Open your jupyter notebook in your terminal by typing "jupyter notebook"
    Open the files you want to run 
    

## Two Major Parts of the Project:
    
    Part I. Build ETL Pipeline for Pre-Processing the Files 
    Part II. Complete the Apache Cassandra coding portion 
    


## Project Steps

1. Modeling the NoSQL database(Apache Cassandra database) 
2. Design tables to answer the queries outlined in the project .ipynb file
3. Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
4. Develop the CREATE statement for each of the tables to address each question
5. Load the data with INSERT statement for each of the tables
6. Include IF NOT EXISTS clauses in the CREATE statements to create tables only if the tables do not already exist. 
    (Include DROP TABLE statement for each table. This way you can run drop and create tables whenever you want to reset your 
    database and test your ETL pipeline ) 
7. Test by running the proper select statements with the correct WHERE clause



## Key Point: Primary Keys in Creating Tables

    There are 3 queries, which means I need to create 3 tables correspondingly. 
        Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
        Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
        Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
 
    Each query requires three basic steps: create a table, shape an incoming row of data, insert that row into the table
    
    It is crucial to create proper primary key, which would directly affect your query execution later. 
    
    Primary key is composed by partion key and clustering columns. The number of clustering columns varies from case to case. Some may require mutiple
    while others may require none. The point you need to keep on mind is that Partition key is for filtering while clustering columns are for sorting.
    
    In query 1: Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338 and itemInSession = 4
        we have two filtering conditions sessionId and itemInSession, so the partition key is a combination of sessionId and itemInSession. 
        Thus PRIMARY KEY((sessionId,itemInSession))
        
    In query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid =     182
         we have two filtering conditions, userid and sessionId, so the partition key is a combination of userid and sessionId. Also, we are required          to sort it by itemInSession. itemInSession should be the clustering column.
         Thus PRIMARY KEY((userid, sessionId), itemInSession)
         
    In query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
        We have only one filtering condition, which is the song title. Song title should be the partition key. Just one partition key may not be     
        enough for the primary key. Since we need user informaion, I add useid as the clustering column.
        Thus PRIMARY KEY(song, userid)


