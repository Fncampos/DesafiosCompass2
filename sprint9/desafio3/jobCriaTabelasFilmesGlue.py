from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
spark = SparkSession.builder.appName("Lendo Parquet").getOrCreate()
#Lendo arquivos
df_tmdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/tmdb/api-movie.parquet/dt=2023-05-21/")
df_imdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/imdb/2023-04-09/movie.parquet/")

#separar colunas de interesse df_tmdb e df_imdb
#Tirando ids duplicados
colunas = ['id', 'imdb_id', 'original_language','popularity','production_companies','production_countries','vote_average','vote_count']
df_tmdb_sel = df_tmdb.select(colunas)
df_tmdb_rn = df_tmdb_sel.withColumnRenamed("id", "id_tmdb") 

colunas = ['id', 'tituloPincipal', 'tituloOriginal','anoLancamento','tempoMinutos','genero','notaMedia', 'numeroVotos']
df_imdb_sel = df_imdb.select(colunas)

colunas2 = ['id_tmdb', 'imdb_id', 'tituloPincipal', 'tituloOriginal','anoLancamento','tempoMinutos','genero','notaMedia', 'numeroVotos','original_language','popularity','production_companies','production_countries','vote_average','vote_count']
df_leftjoin=df_tmdb_rn.join(df_imdb_sel, df_tmdb_rn.imdb_id == df_imdb_sel.id, "left").select(colunas2)
df_leftjoin_duplicates=df_leftjoin.dropDuplicates(["id_tmdb"])

#Extraindo os dados das companias e guardando em Df
df_companies=df_leftjoin_duplicates.select("imdb_id","production_companies")
df_opencompanies=df_companies.withColumn("prod_companies", explode("production_companies")).drop("production_companies")
df_opencompanies_v0=df_opencompanies.select("imdb_id","prod_companies.id", "prod_companies.name").drop("prod_companies.logo_path","prod_companies.orugin_country")
df_opencompanies_v1 = df_opencompanies_v0.withColumnRenamed("id", "id_produtora")

#sExtraindo os dados dos paises das producoes e guardando em Df
df_country=df_leftjoin_duplicates.select("imdb_id","production_countries")
df_opencountry=df_country.withColumn("prod_countries", explode("production_countries")).drop("production_countries")
df_opencountry_v0=df_opencountry.select("imdb_id","prod_countries.iso_3166_1","prod_countries.name")

#criando dimensoes e fato
#dim_filmes
colunas2 = ['id_tmdb', 'imdb_id', 'tituloPincipal', 'tituloOriginal','anoLancamento','tempoMinutos','genero','original_language']
df_filmes=df_leftjoin_duplicates.select(colunas2)
df_filmes1=df_filmes.withColumnRenamed("tituloPincipal", "tituloPrincipal")
df_filmes2=df_filmes1.withColumnRenamed("original_language", "lingua_original")

#fato_avaliacao
df_opencoutry_v1= df_opencountry_v0.withColumnRenamed("imdb_id", "id")
df_fato=df_leftjoin_duplicates.join(df_opencoutry_v1, df_leftjoin_duplicates.imdb_id == df_opencoutry_v1.id,"left").select("*")
colunas3 = ['imdb_id','notaMedia', 'numeroVotos','popularity','vote_average','vote_count',"iso_3166_1","name"]
df_fato_1=df_fato.select(colunas3).drop('production_companies','production_countries')
df_opencompanies_v2= df_opencompanies_v1.withColumnRenamed("imdb_id", "id")
df_fato2=df_fato_1.join(df_opencompanies_v2,df_fato_1.imdb_id==df_opencompanies_v2.id, "left").select("*")
colunas_fato = ['imdb_id','id_produtora','iso_3166_1','popularity','vote_count','vote_average','numeroVotos','notaMedia']
df_fato3=df_fato2.select(colunas_fato).drop('name', 'id')
df_fato4=df_fato3.withColumnRenamed("iso_3166_1", "sigla_pais")
df_fato5=df_fato4.withColumnRenamed("popularity", "pularidade_tmdb")    
df_fato6=df_fato5.withColumnRenamed("vote_count", "contagem_votos_tmdb")    
df_fato7=df_fato6.withColumnRenamed("vote_average", "media_votos_tmdb")  
df_fato8=df_fato7.withColumnRenamed("numeroVotos", "contagem_votos_imdb")  
df_fato9=df_fato8.withColumnRenamed("notaMedia", "media_votos_imdb")  

#dim_produtoras
df_dim_companies=df_opencompanies_v1.dropDuplicates(["id_produtora"]).drop('imdb_id')
df_dim_produtora1=df_dim_companies.withColumnRenamed("name", "nome_produtora")  

#dim_pais
df_dim_pais=df_opencountry_v0.dropDuplicates(["iso_3166_1"]).drop('imdb_id')
df_dim_pais1=df_dim_pais.withColumnRenamed("iso_3166_1", "sigla_pais")
df_dim_pais2=df_dim_pais1.withColumnRenamed("name", "nome_pais")

#Salvando tabelas
df_filmes2.write.saveAsTable("dim_filmes", path="s3://data-lake-flavia/refined/")
df_fato9.write.saveAsTable("fato_avaliacao", path="s3://data-lake-flavia/refined/")
df_dim_produtora1.write.saveAsTable("dim_produtora", path="s3://data-lake-flavia/refined/")
df_dim_pais2.write.saveAsTable("dim_pais", path="s3://data-lake-flavia/refined/")

spark.stop()