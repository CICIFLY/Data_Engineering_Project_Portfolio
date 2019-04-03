# Data_Engineering_Project_Portfolio

## Introduction 
The projects in the Data Engineer Nanodegree program were designed in collaboration with a group of highly talented industry professionals. The projects took me to a role of a Data Engineer at a fabricated data streaming company called “Sparkify” as it scaled its data engineering in both size and sophistication. I worked with simulated data of listening behavior, as well as a wealth of metadata related to songs and artists. At first, I started working with a small amount of data, with low complexity, processed and stored on a single machine. By the end, I developed a sophisticated set of data pipelines to work with massive amounts of data processed and stored on the cloud. There are five projects in the program. Below is a description of each.

## Udacity Data Engineering Course Content
### Course 1: Data Modelling with Postgres and Cassandra
Week #	Lesson
1	Introduction to Data Modeling
2	Relational Data Modeling with Postgres
2	Project: Data Modeling with Postgres
3	Non-Relational Modeling with Cassandra
3	Project: Data Modeling with Cassandra

### Course 2: Cloud Data Warehouses
Week #	Lesson
4	Introduction to the Data Warehouses
5	Introduction to the Cloud and AWS
6	Implementing Data Warehouses on AWS
7	Project: Create a Cloud Data Warehouse

### Course 3: Data Lakes with S3 and Spark
Week #	Lesson
8	The Power of Spark
9	Data Wrangling with Spark
10	Debugging and Optimizing
11	Introduction to Data Lakes
12	Project: Create a Data Lake

### Course 4: Data Pipelines with Airflow
Week #	Lesson
13	Data Pipelines
14	Data Quality
15	Production Data Pipelines
16	Project: Create Data Pipelines
Capstone Project
Week #	Lesson
17	Project: Capstone

## Project 1 - Data Modeling
In this project, I modeled user activity data for a music streaming app called Sparkify. 
### The project was done in two parts.
* I created a database and imported data stored in CSV and JSON files, and model the data. I did this first with a relational model in Postgres, then with a NoSQL data model with Apache Cassandra. I designed the data models to optimize queries for understanding what songs users are listening to. For PostgreSQL, I also defined Fact and Dimension tables and insert data into my new tables. For Apache Cassandra, I modeled your data to help the data team at Sparkify answer queries about app usage. I set up your Apache Cassandra database tables in ways to optimize writes of transactional data on user sessions.

## Project 2 - Cloud Data Warehousing
In this project, you’ll move to the cloud as you work with larger amounts of data. You are tasked with building an ELT pipeline that extracts Sparkify’s data from S3, Amazon’s popular storage system. From there, you’ll stage the data in Amazon Redshift and transform it into a set of fact and dimensional tables for the Sparkify analytics team to continue finding insights in what songs their users are listening to.

## Project 3 - Data Lakes with Apache Spark
In this project, you'll build an ETL pipeline for a data lake. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app. You will load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Project 4 - Data Pipelines with Apache Airflow
In this project, you’ll continue your work on Sparkify’s data infrastructure by creating and automating a set of data pipelines. You’ll use the up-and-coming tool Apache Airflow, developed and open-sourced by Airbnb and the Apache Foundation. You’ll configure and schedule data pipelines with Airflow, setting dependencies, triggers, and quality checks as you would in a production setting.

## Project 5 - Data Engineering Capstone
The capstone project is an opportunity for you to combine what you've learned throughout the program into a more self-driven project. In this project, you'll define the scope of the project and the data you'll be working with. We'll provide guidelines, suggestions, tips, and resources to help you be successful, but your project will be unique to you. You'll gather data from several different data sources; transform, combine, and summarize it; and create a clean database for others to analyze.
