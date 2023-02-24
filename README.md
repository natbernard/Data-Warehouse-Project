# Data-Warehouse-Project
In this project, I built an ETL pipeline that extracts data from S3, stages it in Redshift and then transforms the data into a set of dimensional tables.

## Files in the repository
The repository contains four files; three python scripts and one notebook.
- sql_queries.py contains all the sql queries that are imported by the other two python files.
- create_table.py creates the fact and dimension tables for the star chema in Redshift.
- etl.py loads data from S3 into the staging tables on Redshift and the processes the data onto analytic tables.
- redshift_cluster.ipynb creates clients for ec2, s3, iam, and redshift, creates a redshift cluster and checks the table schemas in the redshift database.

## Running the scripts
The python scripts can be run from the terminal or from a notebook. To run from a terminal, navigate to the location of the script using the 'cd' command then type python name_of_script.py to execute. To run from a notebook, import the python files to the notebook then run the cells.