from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("Lendo Parquet").getOrCreate()

#Lendo arquivos
df_tmdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/tmdb/api-movie.parquet/dt=2023-05-21/")
df_imdb = spark.read.format("parquet").load("s3://data-lake-flavia/trusted/imdb/2023-04-09/movie.parquet/")

#join dos parquets
df_leftjoin=df_tmdb.join(df_imdb, df_tmdb.imdb_id == df_imdb.id,"left").select("*").drop("id").dropDuplicates(["imdb_id"])

#Criando dim_produtoras
df_prod=df_leftjoin.withColumn("prod_companies", explode("production_companies")).drop("production_companies")

colunas = ["prod_companies.id","prod_companies.name"]
df_prod_v1=df_prod.select(colunas).drop("prod_companies.logo_path","prod_companies.origin_country","prod_companies.name").dropDuplicates(["id"])
df_prod_v2=df_prod_v1.withColumnRenamed("id", "id_produtora").withColumnRenamed("name", "nome_produtora")

#Transformando em tabela
spark.sql("use db_filmes")
df_prod_v2.write.saveAsTable("dim_produtoras", path="s3://data-lake-flavia/refined/dim_produtoras/")

spark.stop()