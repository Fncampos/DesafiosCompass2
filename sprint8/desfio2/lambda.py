import json
import boto3
import pandas as pd
import requests

bucket_name = 'data-lake-flavia'
s3_file_name = 'row/2023-04-29/csv/Movies/2023/5/1/movies.csv'
api_key = "8508311b3e6b422030e3fb23336d11b4"
s3_client = boto3.client('s3')

def envia_objeto(dir_local, bucket, dir_s3):
    s3_client.upload_file(dir_local, bucket, dir_s3)
    return 'Arquivo transferido!'
    
def loadCSV(bucket_name, s3_file_name, s3_cliente):
    objeto = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    df1=pd.read_csv(objeto['Body'], sep='|', low_memory=False)
    colunas=["id","genero"]
    df = df1[colunas]
    genero=df[df['genero'].str.contains('Sci-Fi')]
    genero_id= genero['id'].drop_duplicates()
    genero_id = genero_id.dropna()#tipo dt.series
    genero_id= list(genero_id)#tipo lista
    return genero_id
 
def busca_find(id_imdb,api_key):
    try:
        url = f'https://api.themoviedb.org/3/find/{id_imdb}?api_key={api_key}&language=en-US&external_source=imdb_id'
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200:
            return data
        else:
            return None
    except:           
        print(f"Erro ao buscar dados do filme com ID {id_tmdb}: {str(e)}")
        pass

def busca_movie(id_tmdb,api_key):
    try:
        url = f'https://api.themoviedb.org/3/movie/{id_tmdb}?api_key={api_key}&language=en-US'
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200:
            return data
        else:
            return None
    except Exception as e:
        print(f"Erro ao buscar dados da id do TMDB {id_tmdb}: {str(e)}")
        pass

def save_json(json_data, bucket_name, nome, contador):
    nome_arq=nome+str(contador)+".json"
    json_string = json.dumps(json_data)
    json_bytes = json_string.encode('utf-8')
    s3_client.put_object(Body=json_bytes, Bucket=bucket_name, Key=nome_arq)
    print(f"Json n° {contador} gravado no s3")

def separa_idtmdb(request_APIfind):
    try:
        id_tmdb=request_APIfind["movie_results"][0]["id"]
        return id_tmdb
    except KeyError:
        id_tmdb=0
        return id_tmdb
    
def lambda_handler(event, context):
    ind=-1
    count=0
    countjson=1
    resultfind={}
    resultmovie={}

    id_genero=loadCSV(bucket_name, s3_file_name, s3_client)
    for id in id_genero:
        if count==9:
            countjson+=1
            save_json(resultfind, bucket_name, "row/Tmdb/JSON/2023/05/12/find-", countjson)
            resultfind={}
            save_json(resultmovie, bucket_name, "row/Tmdb/JSON/2023/05/12/movie-", countjson)
            resultmovie={}
            count=0
            pass
        else:
            count+=1
            ind+=1
            try:
                find=busca_find(id,api_key)#tipo lista
                if find!=None:
                    id_tmdb=separa_idtmdb(find)#Quando esse id não é encontrado ele dá erro
                    print(f'{ind} - {id_tmdb} - {id}')
                    resultfind[ind] = find
                    movie=busca_movie(id_tmdb,api_key)
                    if movie != None:
                        resultmovie[ind] = movie
                    else:
                        continue
                else:
                    continue               
            except Exception as e:
                print(f"Erro: {e}")
                pass
    countjson+=1
    save_json(resultfind, bucket_name, "row/Tmdb/JSON/2023/05/12/find-", countjson)      
    save_json(resultmovie, bucket_name, "row/Tmdb/JSON/2023/05/12/movie-", countjson)   
        
    return {
        'statusCode': 200,
        'body': f" programa finalizado"
    }
   