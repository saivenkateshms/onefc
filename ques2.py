from pyspark.sql import SparkSession,col
from pyspark.sql.functions import *
from pyspark.sql.functions import to_json, struct


spark = SparkSession \
    .builder \
    .master("local") \
    .appName("onefc") \
    .enableHiveSupport() \
    .getOrCreate()

df_load=spark.read.csv("s3://hvc-aws-infra/data.csv",inferSchema=True,header=True)\
        .withColumnRenamed("Person Id","person_id")\
        .withColumnRenamed("Floor Level","floor_level")\
        .withColumnRenamed("Floor Access DateTime","datetime")

df_group = df_load.groupBy("Building","floor_level","datetime")\
                  .agg(max("person_id").alias("person_id"))\
                  .select("person_id","datetime","floor_level","Building")\
                  .orderBy("building","floor_level","datetime")

df_final = df_group.select(df_group.person_id.cast("string"),df_group.datetime,"floor_level","Building")

'write row by row'
df_load= (df_final.select(to_json(struct([df_final[x] for x in df_final.columns])).alias("value")).write.json("s3://hvc-aws-infra/onefc"))

'write to json whole file'
df_final.write.json("s3://hvc-aws-infra/onefc")
