# Taxi Pipeline

This is a Reposistory for Hafizh's Purwadhika Data Engineering modul 1 capstone project.

## Objective summary:
An ETL to extract data from 2 types of files, CSV and JSON. In each types, the files are seperated into multiple files. The objective is to extract and union all of the files into one single output, and perform transformations to it.

## Steps of the ETL:
1. Extraction: reads CSV and JSON files from data directory
2. Transformation:
    - Normalized column names to use snake_case
    - Calculates trip duration in minutes
    - Change payment type to be readable
    - Converts trip distance from miles to kilometers
3. Loading, extract data into:
    - `staging/` -> for concancenated data without performing transformation
    - `result/` -> for transformed data

## How can I execute the ETL?
Run the ETL process with:
```
python taxi-pipeline/src/main.py
```