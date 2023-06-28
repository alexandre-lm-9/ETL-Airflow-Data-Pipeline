# from pymongo import MongoClient
# import json
# import boto3
# from bson import json_util


def extract_mongodb():
    from pymongo import MongoClient
    import json
    import boto3
    from bson import json_util
    from airflow.models import Variable

    
    # Informações de conexão
    host       = Variable.get('HOST')
    database   = Variable.get('DATABASE')
    collection = Variable.get('COLLECTION')
    username   = Variable.get('USERNAME')
    password   = Variable.get('PASSWORD')

    # Criar a URL de conexão
    url = f"mongodb+srv://{username}:{password}@{host}/{database}?retryWrites=true&w=majority"

    # Conectar ao MongoDB
    client = MongoClient(url)

    # Acessar o banco de dados e a coleção
    db   = client[database]
    coll = db[collection]

    # Agora você pode executar operações no MongoDB, por exemplo:
    documents = coll.find()

    # Extract data from the cursor
    data = list(documents)

    json_data = json.dumps(data, default=json_util.default)

    #Provide your AWS access key ID and secret access key
    access_key = Variable.get('ACCESS_KEY')
    secret_key = Variable.get('SECRET_KEY')

    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    # Upload the JSON string to S3 bucket
    bucket_name           = Variable.get('BUCKET_NAME')
    mongodb_bucket_folder = Variable.get('MONGODB_BUCKET_FOLDER')
    
    s3_client.put_object(Body=json_data, Bucket=bucket_name, Key=f'{mongodb_bucket_folder}/mongodb_data.json')

    # Fechar a conexão com o MongoDB
    client.close()
