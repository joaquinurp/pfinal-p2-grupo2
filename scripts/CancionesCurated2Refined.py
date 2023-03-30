
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
cancionesSource = "s3://pfinal-p2-grupo2/curated/Canciones.csv"
cacionesDestParquet = "s3://pfinal-p2-grupo2/refined/Canciones"
cancionesDestCSV = "s3://pfinal-p2-grupo2/csv/Canciones/Canciones.csv"

dtf_canciones = pd.read_csv(cancionesSource)

dtf_canciones.to_csv(cancionesDestCSV, index=False)

df_canciones = spark.read.option("header", "true").csv(cancionesSource)

df_canciones.write.partitionBy("Usuario").parquet(cacionesDestParquet)

job.commit()