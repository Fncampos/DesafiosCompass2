from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("Lendo Parquet").getOrCreate()

#Lendo arquivos
df_tmdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/tmdb/api-movie.parquet/dt=2023-05-21/")
df_imdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/imdb/2023-04-09/movie.parquet/")

#join dos parquets
df_leftjoin=df_tmdb.join(df_imdb, df_tmdb.imdb_id == df_imdb.id,"left").select("*").drop("id").dropDuplicates(["imdb_id"])

#Criando dim_pais
df_pais=df_leftjoin.withColumn("prod_countries", explode("production_countries")).drop("production_countries") 
colunas = ["prod_countries.iso_3166_1","prod_countries.name"]
df_pais_v1=df_pais.select(colunas).drop("prod_companies.logo_path","prod_companies.origin_country","prod_companies.name").dropDuplicates(["iso_3166_1"])
df_pais_v2=df_pais_v1.withColumnRenamed("iso_3166_1", "sigla_pais").withColumnRenamed("name", "nome_pais")

#Transformando em tabela
spark.sql("use db_filmes")
df_pais_v2.write.saveAsTable("dim_pais", path="s3://data-lake-flavia/refined/dim_pais/")

spark.stop()