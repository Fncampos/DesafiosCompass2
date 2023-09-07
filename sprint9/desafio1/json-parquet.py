from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
spark = SparkSession.builder.appName("Lendo json").getOrCreate()
df=spark.read.option("multiLine", "true").format("json").load("s3://data-lake-flavia/raw/Tmdb/JSON/2023/05/20/api-movie/")
df_with_dt = df.withColumn("dt", lit("2023-05-21"))
df_with_dt.write.partitionBy("dt").parquet("s3://data-lake-flavia/trusted/tmdb/api-movie.parquet/")

spark.stop()