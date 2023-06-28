import requests
import pandas as pd
import boto3
import json
from airflow.models import Variable

def ibge_api():

    # Requesting API data and converting it into a JSON
    response = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/distritos')

    response_json = response.json()

    json_string = json.dumps(response_json)

    #Provide your AWS access key ID and secret access key
    access_key = Variable.get('ACCESS_KEY')
    secret_key = Variable.get('SECRET_KEY')

    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    # Upload the JSON string to S3 bucket
    bucket_name        = Variable.get('BUCKET_NAME')
    ibge_bucket_folder = Variable.get('IBGE_BUCKET_FOLDER')

    s3_client.put_object(Body=json_string, Bucket=bucket_name, Key=f'{ibge_bucket_folder}/ibge_api_data.json')
