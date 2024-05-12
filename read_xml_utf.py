# To download dependencies, execute the following command in the terminal:
# ❯ spark-submit --packages com.databricks:spark-xml_2.12:0.13.0 xml.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize SparkSession

spark = SparkSession.builder.appName("ReadXML").config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()

# Define the path to your XML file
xml_file_path = "cd_catalog.xml"

# Read XML file into DataFrame
df = spark.read \
    .format('com.databricks.spark.xml') \
    .option("rowTag", "CD") \
    .load(xml_file_path)

print("Reading XML...")
# Print DataFrame schema
df.printSchema()

df.show(truncate=False)

# Stop SparkSession
spark.stop()