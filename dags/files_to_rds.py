import pandas as pd
from sqlalchemy import create_engine
import boto3
from airflow.models import Variable

def files_to_rds(file, tablename):

    bucket_name    = Variable.get('BUCKET_NAME')
    mongodb_folder = Variable.get('MONGODB_BUCKET_FOLDER')
    ibge_folder    = Variable.get('IBGE_BUCKET_FOLDER')

    # Provide your AWS access key ID and secret access key
    access_key = Variable.get('ACCESS_KEY')
    secret_key = Variable.get('SECRET_KEY')

    # Checking what data will be readed and inserted into the RDS

    if file == 'mongo':

        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        response = s3_client.get_object(Bucket=bucket_name, Key=f'{mongodb_folder}/mongodb_df.csv')

        mongo_csv_data = response['Body'].read().decode('utf-8')

        mongo_file = pd.DataFrame([value.split('|') for value in mongo_csv_data.split('\n')[1:]], columns=mongo_csv_data.split('\n')[0].split('|'))
        
        mongo_file['idade'] = mongo_file['idade'].fillna('0').astype(int)

        file = mongo_file.loc[(mongo_file['sexo']=='Mulher') & (mongo_file['idade'].between(20, 40))]

    elif file == 'ibge':
        
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        response = s3_client.get_object(Bucket=bucket_name, Key=f'{ibge_folder}/ibge_df.csv')

        ibge_csv_data = response['Body'].read().decode('utf-8')

        ibge_file = pd.DataFrame([value.split('|') for value in ibge_csv_data.split('\n')[1:]], columns=ibge_csv_data.split('\n')[0].split('|'))

        file = ibge_file

    # Database connection details
    db_host     = Variable.get('DB_HOST')
    db_port     = Variable.get('DB_PORT')
    db_name     = Variable.get('DB_NAME')
    db_user     = Variable.get('DB_USER')
    db_password = Variable.get('DB_PASSWORD')

    # Create the database connection string
    db_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Create the SQLAlchemy engine
    engine = create_engine(db_string)

    # Write the DataFrame to the SQL database table
    table_name = tablename
    file.to_sql(table_name, engine, if_exists='replace', index=False)

    # Confirm the write operation
    print('DataFrame written to SQL table successfully.')
