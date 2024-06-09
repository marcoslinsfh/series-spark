# Importando a biblioteca e inicializando a Sessao do Spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Carregamento do Dados

df_device = spark.read.json ("docs/files/device/*.json")


# Schema
df_device.printSchema()

# Colunas
print (df_device.columns)

# Rows
print(df_device.count())

# Select columns
df_device.select ("manufacturer", "model", "platform").show()

# Filtrando dados
df_device.filter (col("manufacturer") == "Lenovo").show()
df_device.filter (df_device.platform == "Android").show()

# Agrupando dados
df_device.groupBy ("manufacturer").count().orderBy("count").show()

