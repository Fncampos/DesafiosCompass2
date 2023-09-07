from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("Lendo Parquet").getOrCreate()

#Lendo arquivos
df_tmdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/tmdb/api-movie.parquet/dt=2023-05-21/")
df_imdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/imdb/2023-04-09/movie.parquet/")

#join dos parquets
df_leftjoin=df_tmdb.join(df_imdb, df_tmdb.imdb_id == df_imdb.id,"left").select("*").drop("id").dropDuplicates(["imdb_id"])

#Criando fato
colunas_fato = ["imdb_id","production_companies","production_countries","vote_average","vote_count","notaMedia", "numeroVotos","popularity"]
df_fato=df_leftjoin.select(colunas_fato)
df_fato_v1=df_fato.withColumn("prod_companies", explode("production_companies")).drop("production_companies").withColumn("prod_countries", explode("production_countries")).drop("production_countries")
colunas_fato = ["imdb_id","prod_companies.id","prod_countries.iso_3166_1","vote_average","vote_count","notaMedia", "numeroVotos","popularity"]
df_fato_v2=df_fato_v1.select(colunas_fato).drop("prod_companies.logo_path","prod_companies.origin_country","prod_companies.name","prod_countries.name")
df_fato_v3=df_fato_v2.withColumnRenamed("iso_3166_1", "sigla_pais").withColumnRenamed("id", "id_produtora").withColumnRenamed("popularity", "popularidade_tmdb").withColumnRenamed("vote_count", "contagem_votos_tmdb").withColumnRenamed("vote_average", "media_votos_tmdb").withColumnRenamed("numeroVotos", "contagem_votos_imdb").withColumnRenamed("notaMedia", "media_votos_imdb")

#Transformando em tabela
spark.sql("use db_filmes")
df_fato_v3.write.saveAsTable("fato_avaliacao", path="s3://data-lake-flavia/refined/fato_avaliacao/")

spark.stop()