
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
s3_path = 's3://pfinal-p2-grupo2/raw/userid-profile.tsv'
import pandas as pd

# Crear DynamicFrame a partir del archivo TSV en S3
df = pd.read_csv(s3_path, delimiter="\t")
df
removedCols = ['gender', 'age']
df = df[df.columns.difference(removedCols)]
df.head()
df.columns
df.count()
# Borramos filas con valores nulos
df = df.dropna()
df.count()
# conversion a fecha y hora
df['registered'] = pd.to_datetime(df['registered'], format='%b %d, %Y')
df['date'] = df['registered'].dt.strftime("%m%d")
df['registered'] = df['registered'].dt.strftime('%Y-%m-%d')
df.head()
print(df.loc[df['#id'] == 'user_000224'])
if 'Congo, the Democratic Republic of the' in df['country'].values:
    df.loc[df['country'] == 'Congo, the Democratic Republic of the', 'country'] = 'Congo'
    print(df.loc[df['#id'] == 'user_000224'])
if "Korea, Democratic People's Republic of" in df['country'].values:
    df.loc[df['country'] == "Korea, Democratic People's Republic of", 'country'] = 'Democratic Republic of Korea'
    print(df.loc[df['#id'] == 'user_000214'])
# ordenamos por fecha ascendente
df = df.sort_values(by='registered', ascending=False)
df.head()
# fecha a estandar ingles sin año 
df.to_csv('s3://pfinal-p2-grupo2/curated/userid-profile.csv', index = False)
s3_dest_path = 's3://pfinal-p2-grupo2/refined/userid-profile/'
df.to_parquet(s3_dest_path, partition_cols=['country'])
job.commit()