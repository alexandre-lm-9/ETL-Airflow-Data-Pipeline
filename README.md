# Data Engineering

## ETL Airflow Data Pipeline

The purpose of this project is to gather different types of data, process and transform them and make it available for a client, exercizing the following concepts:

    1. Data Pipelines.
    2. Containers.
    3. SQL and NoSQL Databases.
    4. Connecting to APIs.
    5. ETL.
    6. Data Lake.
    7. Distributed data processing.

Below are the steps taken in this project. All these steps were taken using Apache Airflow in a Docker Container structure:
• Extract data from a MongoDB database available in the cloud for querying.

• Use an IBGE API (https://servicodados.ibge.gov.br/api/docs/localidades) for extracting information from regions, mesoregions and microregions in Brazil

• Deposit these data in a Datalake in cloud, in this case Amazon Simple Storage Service (Amazon S3) was used.

• After ingesting the data into the Data Lake, some transformations and filters were done.

• Ingest the filtered and handled data in a Datawarehouse (DW) and make it available to the clients to query and analyze the data, in these case Amazon Relational Database Service (Amazon RDS) was used. 

Below you can find the Architect Solution of the project:

![Airflow_Pipeline_Diagram](https://github.com/alexandre-lm-9/ETL-Airflow-Data-Pipeline/assets/123885726/fb76ee7f-8105-4c4e-9a41-f8d843bfa033)
