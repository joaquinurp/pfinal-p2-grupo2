
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

users_schema = StructType() \
      .add("#id",StringType(),True) \
      .add("country",StringType(),True) \
      .add("registered",StringType(),True) \
      .add("date",IntegerType(),True)
      
horoscope_schema = StructType() \
      .add("Horoscope",StringType(),True) \
      .add("TimeStart",StringType(),True) \
      .add("TimeEnd",StringType(),True) \
      .add("TimeStartInt",IntegerType(),True) \
      .add("TimeEndInt",IntegerType(),True)


usersSource = "s3://pfinal-p2-grupo2/curated/userid-profile.csv"
horoscopeSource = "s3://pfinal-p2-grupo2/curated/horoscopeCured.csv"
countriesSource = "s3://pfinal-p2-grupo2/curated/countryContinentCured.csv"

df_users = spark.read.option("header", "true").schema(users_schema).csv(usersSource)
df_horoscope = spark.read.option("header", "true").schema(horoscope_schema).csv(horoscopeSource)
df_countries = spark.read.option("header", "true").csv(countriesSource)
df_user_countries = df_users.join(df_countries, df_users.country == df_countries.country, 'left').drop(df_users.country)
from pyspark.sql import functions as F
df_hor_users_country = df_user_countries.join(df_horoscope, F.col("date").between(F.col('TimeStartInt'), F.col('TimeEndInt')), 'left').drop("TimeStart", "TimeEnd", "TimeStartInt", "TimeEndInt", "date")
df_hor_users_country.write.partitionBy("country").parquet("s3://pfinal-p2-grupo2/refined/TablaMaestra/tablamaestra.parquet")

df_hor_users_country.write.csv("s3://pfinal-p2-grupo2/csv/TablaMaestra")
job.commit()