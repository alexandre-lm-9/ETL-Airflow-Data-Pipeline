import pandas as pd
import boto3
import json
from airflow.models import Variable



def mongo_to_df():

    bucket_name    = Variable.get('BUCKET_NAME')
    mongodb_folder = Variable.get('MONGODB_BUCKET_FOLDER')

    #Provide your AWS access key ID and secret access key
    access_key = Variable.get('ACCESS_KEY')
    secret_key = Variable.get('SECRET_KEY')

    s3_client  = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    response   = s3_client.get_object(Bucket=bucket_name, Key=f'{mongodb_folder}/mongodb_data.json')
        
    # Get the content of the file
    content   = response['Body'].read().decode('utf-8')
    documents = json.loads(content)

    # Iterate the documents and saving each item as a lit
    id        = [each['_id']['$oid'] for each in documents]
    ano       = [each['ano'] for each in documents]
    trimestre = [each['trimestre'] for each in documents]
    uf        = [each['uf'] for each in documents]
    sexo      = [each['sexo'] for each in documents]
    idade     = [each['idade'] for each in documents]
    cor       = [each['cor'] for each in documents]
    graduacao = [each['graduacao'] for each in documents]
    trab      = [each['trab'] for each in documents]
    ocup      = [each['ocup'] for each in documents]
    renda     = [each['renda'] for each in documents]
    horastrab = [each['horastrab'] for each in documents]
    anosesco  = [each['anosesco'] for each in documents]

    # Creating a dataframe with the lists as columns

    df = pd.DataFrame(list(zip(id, 
                            ano,
                            trimestre,
                            uf,
                            sexo,
                            idade,
                            cor,
                            graduacao,
                            trab,
                            ocup,
                            renda,
                            horastrab,
                            anosesco)), columns=['id',
                                                 'ano',
                                                 'trimestre',
                                                 'uf',
                                                 'sexo',
                                                 'idade',
                                                 'cor',
                                                 'graduacao',
                                                 'trab',
                                                 'ocup',
                                                 'renda',
                                                 'horas_trab',
                                                 'anosesco'])
    
    df.to_csv('/opt/airflow/data/mongodb_df.csv', index=False, sep='|')

    s3_client.upload_file('/opt/airflow/data/mongodb_df.csv',bucket_name, #bucket name
                                  f'{mongodb_folder}/mongodb_df.csv')


def ibge_to_df():


    bucket_name = Variable.get('BUCKET_NAME')
    ibge_folder = Variable.get('IBGE_BUCKET_FOLDER')

     #Provide your AWS access key ID and secret access key
    access_key = Variable.get('ACCESS_KEY')
    secret_key = Variable.get('SECRET_KEY')

    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    response  = s3_client.get_object(Bucket=bucket_name, Key=f'{ibge_folder}/ibge_api_data.json')
        
    # Get the content of the file
    content = response['Body'].read().decode('utf-8')
    response_json = json.loads(content)

    distrito_id    = [each['id'] for each in response_json]
    distrito_nome  = [each['nome'] for each in response_json]
    municipio_id   = [each['municipio']['id'] for each in response_json]
    municipio_nome = [each['municipio']['nome'] for each in response_json]
    microrreg_id   = [each['municipio']['microrregiao']['id'] for each in response_json]
    microrreg_nome = [each['municipio']['microrregiao']['nome'] for each in response_json]
    mesorreg_id    = [each['municipio']['microrregiao']['mesorregiao']['id'] for each in response_json]
    mesorreg_nome  = [each['municipio']['microrregiao']['mesorregiao']['nome'] for each in response_json]
    uf_id          = [each['municipio']['microrregiao']['mesorregiao']['UF']['id'] for each in response_json]
    uf_sigla       = [each['municipio']['microrregiao']['mesorregiao']['UF']['sigla'] for each in response_json]
    uf_nome        = [each['municipio']['microrregiao']['mesorregiao']['UF']['nome'] for each in response_json]
    regiao_id      = [each['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['id'] for each in response_json]
    regiao_sigla   = [each['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['sigla'] for each in response_json]
    regiao_nome    = [each['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['nome'] for each in response_json]

    # Creating a dataframe with the lists as columns

    df = pd.DataFrame(list(zip(distrito_id, 
                            distrito_nome,
                            municipio_id,
                            municipio_nome,
                            microrreg_id,
                            microrreg_nome,
                            mesorreg_id,
                            mesorreg_nome,
                            uf_id,
                            uf_sigla,
                            uf_nome,
                            regiao_id,
                            regiao_sigla,
                            regiao_nome)), columns=['distrito_id',
                                                    'distrito_nome',
                                                    'municipio_id',
                                                    'municipio_nome',
                                                    'microrreg_id',
                                                    'microrreg_nome',
                                                    'mesorreg_id',
                                                    'mesorreg_nome',
                                                    'uf_id',
                                                    'uf_sigla',
                                                    'uf_nome',
                                                    'regiao_id',
                                                    'regiao_sigla',
                                                    'regiao_nome'])


    df.to_csv('/opt/airflow/data/ibge_df.csv', index=False, sep = '|')

    bucket_name = Variable.get('BUCKET_NAME')
    ibge_folder = Variable.get('IBGE_BUCKET_FOLDER')

    s3_client.upload_file('/opt/airflow/data/ibge_df.csv',bucket_name,
                                  f'{ibge_folder}/ibge_df.csv')
