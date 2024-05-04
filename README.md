# Movie Data Transformation Pipeline with PySpark and Databricks

## Project Overview:

This project is a PySpark script designed to run on Databricks, a unified data analytics platform. The script performs data transformation and cleaning on movie data stored in JSON files. It reads the movie data from multiple JSON files, applies data cleaning operations, creates lookup tables for genres and languages, handles missing genre names, and deduplicates the data. The cleaned and transformed data is then saved as Delta tables for further processing or analysis.

## Prerequisites

- Access to a Databricks workspace
- JSON files containing movie data uploaded to DBFS (Databricks File System)

## Setup

1. Create a new Databricks notebook or update an existing one.
2. Copy and paste the PySpark script from `movie_data_transformation.py` into a new cell in the notebook.
3. Update the `dbfs_path` variable with the appropriate path to the directory containing your JSON files on DBFS.


## Instructions

1. Attach the notebook to a Databricks cluster with PySpark enabled.
2. Run the notebook cell containing the PySpark script.
3. The cleaned and transformed movie data will be saved as Delta tables in the specified DBFS location.

## Project Structure

- `movie_data_transformation.py`: The main PySpark script that performs data transformation and cleaning operations.
- `data/movie_*.json`: JSON files containing the movie data to be processed (uploaded to DBFS).

## Key Features

- Reads movie data from multiple JSON files on DBFS and converts them into PySpark DataFrames.
- Performs data cleaning operations, such as handling negative runtime values and setting a minimum budget.
- Creates lookup tables for genres and languages based on the movie data.
- Fixes missing genre names by joining the movie data with the genres lookup table.
- Handles duplicate records in the movie data.
- Saves the cleaned and transformed movie data, genres lookup table, and languages lookup table as Delta tables on DBFS for efficient storage and querying.

## Dependencies

- Databricks Runtime with PySpark enabled
