# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from awsglue import DynamicFrame
 
# from pyspark.sql.functions import col, length, trim, lower, size, transform, array, explode
# from pyspark.sql.types import StructType, StructField, StringType, ArrayType
 
# args = getResolvedOptions(sys.argv, ["JOB_NAME"])
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args["JOB_NAME"], args)
 
# keywordsdf = spark.read.csv(
#     "s3://cyble-keywords/allkeywords/all_keyword.csv").distinct().filter(
#         length(col("_c0")) >= 4)
# keywordsdf = keywordsdf.withColumn('_c0', lower(keywordsdf['_c0']))
# keywordslist = keywordsdf.select('_c0').rdd.flatMap(lambda x: x).collect()
 
# datarecordsdf = spark.read.json(
#     "s3://mlrbucketdemo/input_folder/30_million/40_million/")
 
# def process_data(row):
#     data, date, s3_key = row
#     matchedkeywords = [
#         keyword for keyword in keywordslist if keyword in data.lower()
#     ]
#     return data, date, s3_key, matchedkeywords
 
# schema = StructType([
#     StructField("data", StringType(), True),
#     StructField("date", StringType(), True),
#     StructField("s3_key", StringType(), True),
#     StructField("matchedkeywords", ArrayType(StringType()), True)
# ])
# matchedrecordsdf = spark.createDataFrame(
#     datarecordsdf.rdd.map(process_data),
#     schema=schema).filter(size(col("matchedkeywords")) > 0)
 
# finaldf = matchedrecordsdf.withColumn("keyword", explode("matchedkeywords")).drop("matchedkeywords")
# finaldf.write.partitionBy('date').mode('append').format('parquet').save('s3://send2cyble/output/gluejob/')
 
# job.commit()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder.appName("YourAppName").getOrCreate()
# # Read keywords data
# keywordsdf = spark.read.csv("s3://cyble-keywords/allkeywords/all_keyword.csv").distinct().filter(
#     length(col("_c0")) >= 4
# )
# keywordsdf = keywordsdf.withColumn("_c0", lower(keywordsdf["_c0"]))
# keywordslist = keywordsdf.select("_c0").rdd.flatMap(lambda x: x).collect()

# Read data records
# datarecordsdf = spark.read.json("C:\Users\VenkataGurnadhamRavu\Documents\pyspark")
# keywordslist = ["keyword1","keyword2","keyword3"]

# # Define a function to process data
# def process_data(row):
#     data, date, s3_key = row
#     matchedkeywords = [keyword for keyword in keywordslist if keyword in data.lower()]
#     return data, date, s3_key, matchedkeywords

# Define the schema for the final DataFrame
# schema = StructType([
#     StructField("data", StringType(), True),
#     StructField("date", StringType(), True),
#     StructField("s3_key", StringType(), True),
#     StructField("matchedkeywords", ArrayType(StringType()), True)
# ])

# # Create a DataFrame using the process_data function and the defined schema
# matchedrecordsdf = spark.createDataFrame(datarecordsdf.rdd.map(process_data), schema=schema).filter(
#     size(col("matchedkeywords")) > 0
# )

# # Explode the matchedkeywords array and drop the original column
# finaldf = matchedrecordsdf.withColumn("keyword", explode("matchedkeywords")).drop("matchedkeywords")

# # Write the final DataFrame to S3 partitioned by date in Parquet format
# finaldf.write.partitionBy("date").mode("append").parquet("s3://send2cyble/output/emrjob/")

# # Stop the SparkSession
# spark.stop()


# datarecordsdf = spark.read.json("C:\Users\VenkataGurnadhamRavu\Documents\pyspark")
keywordslist = ["keyword1","keyword2","keyword3"]
data = [
    ("This is a sample text with keyword1.", "2022-01-01", "key1"),
    ("Another text mentioning Keyword2.", "2022-01-02", "key2"),
    ("No keywords here.", "2022-01-03", "key3"),
    ("Text with multiple keywords: keyword1, keyword2, keyword3.", "2022-01-04", "key4")
]

# Define a function to process data
# def process_data(row):
#     data, date, s3_key = row
#     matchedkeywords = [keyword for keyword in keywordslist if keyword in data.lower()]
#     return data, date, s3_key, matchedkeywords

# Define the schema for the final DataFrame
schema = StructType([
    StructField("data", StringType(), True),
    StructField("date", StringType(), True),
    StructField("s3_key", StringType(), True)
    # StructField("matchedkeywords", ArrayType(StringType()), True)
])
datarecordsdf = spark.createDataFrame(data, schema=schema)
datarecordsdf = datarecordsdf.withColumn("data", col("data").cast("string"))
# Create a DataFrame using the process_data function and the defined schema
# matchedrecordsdf = spark.createDataFrame(datarecordsdf.rdd.map(process_data), schema=schema).filter(
#     size(col("matchedkeywords")) > 0
# )
pattern = "|".join(keywordslist)
print(type(pattern))
matchedrecordsdf = (
    datarecordsdf
    .withColumn("matchedkeywords", regexp_extract(col("data"), pattern,0))
    .withColumn("matchedkeywords", split(col("matchedkeywords"), ","))
    .filter(size(col("matchedkeywords")) > 0)
    .select("data", "date", "s3_key", "matchedkeywords")
)

matchedrecordsdf.show(truncate=False)


# # Explode the matchedkeywords array and drop the original column
# finaldf = matchedrecordsdf.withColumn("keyword", explode("matchedkeywords")).drop("matchedkeywords")

# # Write the final DataFrame to S3 partitioned by date in Parquet format
# finaldf.write.partitionBy("date").mode("append").parquet("s3://send2cyble/output/emrjob/")

# # Stop the SparkSession
spark.stop()