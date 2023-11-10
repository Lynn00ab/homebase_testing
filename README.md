## Data Ingestion and Transformation with Python
### Ingestion
I chose bank marketing dataset. It has 17 variables. You can see details of dataset at: https://archive.ics.uci.edu/dataset/222/bank+marketing

### Transformation
I perform data validation through the following steps:
- Explore the dataset: Check the data type of each column and verify if there are any null values.
- Check the distribution of classes within each categorical column.
- Verify if the dataset contains NaN values.
- Examine for any duplicate data entries.
- Inspect binary data types and assess the possibility of optimization.
- Select a column as an index and primary key.

### Statistics 
Analyze the correlation between three variables: in this case, I have chosen age, balance, and housing.
#### See code in script "ingestion_and_transformation.iypnb"" 

## Database Storage with PostgreSQL
### Create a PostgreSQL database and table.
The database and table creation script is located in the 'postgre_query.sql' .
I have added the 'created_at' and 'updated_at' columns to record the timestamps when data is inserted and trigger when data is updated, updating the 'updated_at' column accordingly.
A trigger is set up to update the 'updated_at' column with the current timestamp when data is updated.
The database owner privileges are assigned.

### Store the cleaned data
I initialize the storage of initial data in the "data_to_postgre.ipynb" script.
Utilize Python for data insertion into Postgre.

## Data Transfer with Airflow
Before executing the hourly transfer using Airflow, it is necessary to input the initial data into the ClickHouse database. This is accomplished through the "clickhouse_init.ipynb" script.
Afterward, create a DAG for scheduling. Assuming the data is stable in PostgreSQL database, the 'created_at' field is used as a condition for writing to ClickHouse database. Refer to the "Airflow_DAG.py" script.

## Data Querying with ClickHouse
Assuming variable X corresponds to the 'job' field, variable Y to the 'balance' field, and variable Z to the 'married' field. Refer to the queries in the "clickhouse_queries.sql" script.





