import pandas as pd
import json
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import pyarrow
from dotenv import load_dotenv
import os

'''EXTRACT AND TRANSFORM'''

with open('anonymized_collector_selector_data.json', 'r') as f: #Load el archivo JSON
    collector_selector = json.load(f)

with open('anonymized_raw_conversations_data.json', 'r') as f: #Load el archivo JSON
    raw_conversations = json.load(f)

users_data = []
questions_data = []
answers_data = []
conversations_data = []

for user_id, user_info in collector_selector.items():
    users_data.append({
        'id': user_id,
        'modified_date': user_info['modified_date'],
        'score': user_info['score'],
        'who_end_conversation': user_info['who_end_conversation'],
        'brief_score_explanation': user_info['brief_score_explanation'],
        'discarded': user_info['discarded']
    })
    
    for question_id, question_info in user_info['question_answers'].items():
        questions_data.append({
            'id': question_id,
            'text': question_info['text'],
            'type': question_info['type'],
            'order': question_info['order']
        })
        
        answers_data.append({
            'id': f"{user_id}"+"_"+f"{question_id}",
            'user_id': user_id,
            'question_id': question_id,
            'answer': question_info['answer']
        })
    
    for conversation in raw_conversations[user_id]:
        conversations_data.append({
            'id': f"{user_id}"+"_"+f"{conversation['role']}",
            'user_id': user_id,
            'role': conversation['role'],
            'content': conversation['content']
        })

users_df = pd.DataFrame(users_data)
questions_df = pd.DataFrame(questions_data)
answers_df = pd.DataFrame(answers_data)
conversations_df = pd.DataFrame(conversations_data)

users_df.to_parquet('df1.parquet')
questions_df.to_parquet('df2.parquet')
answers_df.to_parquet('df3.parquet')
conversations_df.to_parquet('df4.parquet')

'''LOAD'''

load_dotenv()

credentials = service_account.Credentials.from_service_account_file(os.getenv("credentials_JSON_Path"))

storage_client = storage.Client(credentials=credentials) # Configura el cliente de Google Cloud Storage
bucket_name = os.getenv("bucket_name")
bucket = storage_client.get_bucket(bucket_name)

blob1 = bucket.blob('df1.parquet') # Sube los archivos Parquet a GCS
blob1.upload_from_filename('df1.parquet')

blob2 = bucket.blob('df2.parquet')
blob2.upload_from_filename('df2.parquet')

blob3 = bucket.blob('df3.parquet')
blob3.upload_from_filename('df3.parquet')

blob4 = bucket.blob('df4.parquet')
blob4.upload_from_filename('df4.parquet')


client = bigquery.Client(credentials=credentials) #--> Construct a BigQuery client object.

table_id = [
    os.getenv('users_table_id'),
    os.getenv('questions_table_id'),
    os.getenv('answers_table_id'),
    os.getenv('conversations_table_id')
]

dataset_id = os.getenv('data_set')

job_config = bigquery.LoadJobConfig()

job_config.source_format = bigquery.SourceFormat.PARQUET

job_config.autodetect = True # Detecta automáticamente el esquema

for i in range(1, 5):
    uri = f"gs://{bucket_name}/df{i}.parquet"
    load_job = client.load_table_from_uri(
        uri, f"{dataset_id}"+'.'+ table_id[i-1], job_config=job_config
    )
    load_job.result() # --> Espera a que el trabajo se complete
    destination_table = client.get_table(f"{dataset_id}"+'.'+ table_id[i-1]) # --> Obtiene la tabla de destino y muestra el número de filas cargadas
    print("Loaded {} rows.".format(destination_table.num_rows))

print("se ha terminado la carga")