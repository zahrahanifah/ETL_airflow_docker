# Data Engineering 
## Project: Data Pipelines with Airflow, Postgresql, Docker
## Table of Contents
* **Definition**
    * **Project Overview** :
     We will use data from Yelp to work on this project
     Yelp is an online review platform, where users can leave a star rating and a detailed review on a business. User can leave any Review or Tip, and we can find business's profile and user's profile
   
* **Design**
* **ETL Design Principles**
    1. Read source data (.json and .csv file) and ingest it into ODS layer
    2. From ODS layer to DWH layer (Fact and Dim)
    3. From DWH layer to Serving layer
        
* **Operators** :
   For this project, I have build five different operators that will stage the data and transform the data
   * **read_json:** expected to be able to load any JSON file to python dataframe
   * **load_staging:** expected to be able to load python dataframe to staging table in Postgresql
   * **load_fact:** expected to be able to load staging table to fact table in Postgresql
   * **load_dim:** expected to be able to load staging table to dim table in Postgresql
   * **load_serving:** expected to be able to load fact table and dim table to serving table in Postgresql

* **How to Run** : Open the terminal, type as below
    1.  docker build . --tag extending_airflow:latest


