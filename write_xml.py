from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("WriteXML").config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()

data = [(1, "John"), (2, "Jane"), (3, "Jim")]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

df = spark.createDataFrame(data, schema)

xmlFile = "output/xml/file.xml"

df \
    .select(struct("id", "name").alias("root")) \
    .coalesce(1) \
    .write \
    .format('com.databricks.spark.xml') \
    .option('rootTag', 'root') \
    .option('rowTag', 'row') \
    .save(xmlFile)

# Stop SparkSession
spark.stop()