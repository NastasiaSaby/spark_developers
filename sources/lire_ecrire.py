# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType

data = [("Nastasia", "Saby", "Ingénieur ML"),
    ("Tommy", "Rose", "Ingénieur informatique"),
    ("Megan", "Williams","Juriste"),
    ("Ismaël", "Jones", "Service à la personne"),
    ("Mathieu", "Brown","Ingénieur génie civil")
  ]

schema = StructType([ \
    StructField("prenom", StringType(), True), \
    StructField("nom", StringType(), True), \
    StructField("metier", StringType(), True) \
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
df.show(truncate=False)

# COMMAND ----------

df.write.mode("overwrite").format("csv").option("path", "/mnt/data/personnes").save()

# COMMAND ----------

dbutils.fs.ls("/mnt/data/personnes")

# COMMAND ----------

nouvelle_df = spark.read.option("mode", "failfast").option("path", "/mnt/data/personnes").format("csv").load()

# COMMAND ----------

nouvelle_df.show()

# COMMAND ----------

nouvelle_df.printSchema()
