# Databricks notebook source
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

data: List = [("Diamant_1A", "TopDiamant"),
    ("Diamant_2B", "Diamants pour toujours"),
    ("Diamant_3C", "Mes diamants préférés"),
    ("Diamant_4D", "Diamants que j'aime"),
    ("Diamant_5E", "TopDiamant")
  ]

schema: StructType = StructType([ \
    StructField("reference", StringType(), True), \
    StructField("marque", StringType(), True),
  ])

dataframe: DataFrame = spark.createDataFrame(data=data,schema=schema)
  
dataframe.show(truncate=False)

# COMMAND ----------

dataframe.write.mode("overwrite").format("csv").option("path", "/mnt/data/diamants").save()

# COMMAND ----------

dbutils.fs.ls("/mnt/data/diamants")

# COMMAND ----------

nouvelle_dataframe = spark.read.option("mode", "failfast").option("path", "/mnt/data/diamants").format("csv").load()

# COMMAND ----------

nouvelle_dataframe.show()

# COMMAND ----------

nouvelle_dataframe.printSchema()

# COMMAND ----------


