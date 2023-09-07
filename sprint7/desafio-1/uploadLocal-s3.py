import boto3
import time 
from datetime import date
import os 
from dotenv import load_dotenv
load_dotenv()

#dir_pasta='/app/dados' #para usar no docker
dir_pasta='/home/flavia/Documentos/CompassUOL/compassGitHub/repoGitHub-compass/compass/Transfer' 

bucket_name = "data-lake-flavia"

def upload_file(file_name, bucket, object_name):
    s3_client = boto3.client('s3',
        aws_access_key_id= os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        aws_session_token=os.getenv('aws_session_token')
        )
    s3_client.upload_file(file_name, bucket, object_name)
    print(f'Arquivo transferido!')

def data_criacao(arquivo):
    path = f"{dir_pasta}/{arquivo}"
    ti_m = os.path.getctime(path)
    m_ti = time.ctime(ti_m)
    t_obj = time.strptime(m_ti)
    T_stamp = time.strftime("%Y-%m-%d", t_obj)
    print(f"O arquivo localizado no path: {path}\nCriado em: {T_stamp}")
    return f'{T_stamp}'

def identifica_arquivo(dir_pasta):
    listaArquivos = os.listdir(dir_pasta)
    print("arquivos encontrados:",listaArquivos)
    return listaArquivos 

def info_arquivo(arquivo):
    print(f'Coletando dados... {arquivo}')
    data_origin= data_criacao(arquivo)
    data_format=arquivo.split(".")[-1]
    data_specification=arquivo.split(".")[0].capitalize()
    data_atual = date.today()
    year= data_atual.year
    month= data_atual.month
    day= data_atual.day
    storage_layer = "raw-teste"
    object_name = f'{storage_layer}/{data_origin}/{data_format}/{data_specification}/{year}/{month}/{day}/{arquivo}'
    movies_file_path= f'{dir_pasta}/{str(arquivo)}'
    print(f'Transferindo {arquivo} para Bucket {bucket_name}...')
    upload_file(movies_file_path, bucket_name, object_name)
   
print(f"Esse programa carrega Objetos do diretório '{dir_pasta}' para o DataLake '{bucket_name}'")
lista=identifica_arquivo(dir_pasta)
for item in lista:
    info_arquivo(item)

print(f'Programa Finalizado!!!')

