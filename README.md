# Project Title: Analysis of Renewable Energy Development and Its Impact on Carbon Emissions

## Introduction
Welcome to the repository for our research project on renewable energy development and its impact on global carbon emissions. This project aims to analyze trends and patterns in renewable energy investments, generation volumes, and CO2 emissions across different countries from 2000 to 2016. Utilizing advanced big data analytics tools such as Hadoop and Spark, we delve into the intricate relationship between renewable energy development and carbon emissions, seeking to offer meaningful insights into this critical area.

## Objective
The primary objective of this project is to:

Examine renewable energy development trends globally.
Analyze the impact of these trends on global CO2 emissions.
Identify key factors contributing to the changes in renewable energy development and carbon emissions.

## Folder Structure
data_ingestion: Contains scripts used for ingesting the datasets into our analysis pipeline.
data_cleaning: Features scripts for cleaning and preparing the data for analysis.
ana_code: This folder hosts the main analytics code that performs the bulk of the analysis.
profiling_code: Separate code for data profiling, not included in the main analytics code.

## Access to Datasets
Datasets are accessible to users with specific credentials. Currently, access is granted to as17321_nyu_edu, lj2330_nyu_edu, and adm209_nyu_edu. The datasets (fossil_co2.csv, re_generation.csv, investment_data.csv) are located in the ak8827 user’s folder named “project”.

## Running and Testing the Codes
1. Download the Datasets and Codes:
2. Ensure you have access to the datasets.
3. Download the codes for analysis from the ana_code and profiling_code folders.

Update File Paths:

Since the codes were developed independently, paths to datasets might differ. Update these paths in your local environment.
Running the Code:

To run the program, use Spark’s shell command:

spark-shell --deploy-mode client -i <name of the scala file>

## Contribution
We encourage contributions to this project! If you're interested in contributing, please:

Fork the repository.
Create a new branch for your feature or fix.
Commit your changes.
Push to the branch.
Create a new Pull Request.
