# Project introduction
Use AWS redshift to build an ETL pipeline for a database.
Load data from S3 to staging tables on Redshift 
and execute SQL statements that create the analytics tables from these staging tables.

## How to run
- Create connection to Redshift. Run redshift.py
** Please do not run redshift.py file because we have initial redshift already.
- Add IAM Role by run create_iam_role.py
- Create database connection by run create_tables.py
** Please do not run create_tables.py because we have all databases
- Read etl to run load CSV file from S3, then insert to own database (staging, fact and dimension)

** Run delete_cluster.py will delete all redshift. Please carefully.