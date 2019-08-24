
## Project Instruction 

### Step 1: Scope the Project and Gather Data

    Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, you’ll:

    Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows). See Project Resources for ideas of what data you can use.

    Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

### Step 2: Explore and Assess the Data

    Explore the data to identify data quality issues, like missing values, duplicate data, etc.
    Document steps necessary to clean the data

## Step 3: Define the Data Model

    Map out the conceptual data model and explain why you chose that model
    List the steps necessary to pipeline the data into the chosen data model

## Step 4: Run ETL to Model the Data

    * Create the data pipelines and the data model
    * Include a data dictionary
    * Run data quality checks to ensure the pipeline ran as expected
    * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
    * Unit tests for the scripts to ensure they are doing the right thing
    * Source/count checks to ensure completeness
    
## Step 5: Complete Project Write Up

    What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated?     Why did you choose the model you chose?
    Clearly state the rationale for the choice of tools and technologies for the project.
    Document the steps of the process.
    Propose how often the data should be updated and why.
    Post your write-up and final data model in a GitHub repo.
    Include a description of how you would approach the problem differently under the following 
     
## scenarios:
    If the data was increased by 100x.
    If the pipelines were run on a daily basis by 7am.
    If the database needed to be accessed by 100+ people.




## Datasets

    ### Brief Introduction of the dataset 
    
    The following 4 datasets are included in the project.If something about the data is unclear, make an assumption, document it, and move on. 
    (1). I94 Immigration Data: This data comes from the US National Tourism and Trade Office. 
    A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.
    (2). World Temperature Data: This dataset came from Kaggle. 
    (3). U.S. City Demographic Data: This data comes from OpenSoft. 
    (4).Airport Code Table: This is a simple table of airport codes and corresponding cities. 

    ### Accessing the Data
    The immigration data and the global temperate data is in an attached disk.
    (1). Immigration Data
        You can access the immigration data in a folder with the following path: 
            ../../data/18-83510-I94-Data-2016/. 
        There's a file for each month of the year. 
    
        An example file name is i94_apr16_sub.sas7bdat. Each file has a three-letter abbreviation  
        for the month name. 
        So a full file path for June would look like this: 
            ../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat. 
    
        Below is what it would look like to import this file into pandas. (Note: these files are   
        large, so you'll have to think about how to process and aggregate them efficiently.)
        fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'   
        df = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")
    
    ### The most important decision for modeling with this data is thinking about the level of aggregation. Do you want to aggregate by airport by month? Or by city by year? This level of aggregation will influence how you join the data with other datasets. There isn't a right answer, it all depends on what you want your final dataset to look like.

    (2). Temperature Data
        You can access the temperature data in a folder with the following path: 
            ../../data2/. 
        There's just one file in that folder, called  GlobalLandTemperaturesByCity.csv. 
            
        Below is how you would read the file into a pandas dataframe.
        fname = '../../data2/GlobalLandTemperaturesByCity.csv'
        df = pd.read_csv(fname)