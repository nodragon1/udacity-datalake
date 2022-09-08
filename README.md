## PURPOSE OF THE DATABASE

The startup, Sparkify wants to extract datasets from udacity bucket and store it their own bucket using emr cluster. They need to build ETL pipeline to extract data and transform it into a star schema. Lastly, they store the data schema into parquet file form.

## EXPLANATION OF THE FILES

#### dl.cfg
It contains access key and secret access key of IAM user

#### etl.py
It executes all the process we need

## STEPS FOR PROJECT
1. create iam user with access role
2. fill dl.cfg file based on the iam user's information
3. write paths of datasets and create each table
4. get output path for storing parquet files

## DATABASE SCHEMA DESIGN

<center>
  <img
    src="erd.png"
    width="700"
    height="1400"
  />
</center>