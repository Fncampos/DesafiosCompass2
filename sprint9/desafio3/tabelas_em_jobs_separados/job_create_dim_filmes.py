from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("Lendo Parquet").getOrCreate()

#Lendo arquivos
df_tmdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/tmdb/api-movie.parquet/dt=2023-05-21/").withColumnRenamed("id","tmdb_id")
df_imdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/imdb/2023-04-09/movie.parquet/")

#join dos parquets
df_leftjoin=df_tmdb.join(df_imdb, df_tmdb.imdb_id == df_imdb.id,"left").select("*").dropDuplicates(["imdb_id"])

#Criando dim_filmes
colunas = ["imdb_id" ,"tmdb_id","tituloPincipal","tituloOriginal","anoLancamento","genero","tempoMinutos","original_language"]
df_filmes=df_leftjoin.select(colunas)
df_filmes_v2=df_filmes.withColumnRenamed("tituloPincipal", "tituloPrincipal").withColumnRenamed("original_language", "lingua_original")

#Transformando em tabela
spark.sql("use db_filmes")
df_filmes_v2.write.saveAsTable("dim_filmes", path="s3://data-lake-flavia/refined/dim_filmes/")

spark.stop()