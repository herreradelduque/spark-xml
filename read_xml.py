# To download dependencies, execute the following command in the terminal:
# ❯ spark-submit --packages com.databricks:spark-xml_2.12:0.13.0 xml.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize SparkSession

spark = SparkSession.builder.appName("ReadXML").config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()

#xmlFile = "books.xml"
xmlFile = "books.xml"

df = spark.read \
    .format('com.databricks.spark.xml') \
    .option("rowTag", "book") \
    .option("treatEmptyValuesAsNulls", "true") \
    .load(xmlFile)

print("Reading XML...")
# Print DataFrame schema
df.printSchema()

df.show(truncate=False)

# Stop SparkSession
spark.stop()