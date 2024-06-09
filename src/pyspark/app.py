# Importando a biblioteca e inicializando a Sessao do Spark

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Carregamento do Dados

df_device = spark.read.json ("docs/files/device/device_2022_6_7_19_39_24.json")


# Schema
df_device.printSchema()

# Colunas
print (df_device.columns)

# Rows
print(df_device.count())

# Select columns
df_device.select ("manufacturer", "model", "platform").show()


