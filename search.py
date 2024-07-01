from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, lit

# Create SparkSession
spark = SparkSession.builder.appName("KeywordExtraction").getOrCreate()

# Define sample data and keywords
keywordslist = ["keyword1", "keyword2", "keyword3"]
data = [
    ("This is a sample text with keyword1.", "2022-01-01", "key1"),
    ("Another text mentioning Keyword2.", "2022-01-02", "key2"),
    ("No keywords here.", "2022-01-03", "key3"),
    ("Text with multiple keywords: keyword1, keyword2, keyword3.", "2022-01-04", "key4")
]

# Define schema for DataFrame
schema = ["data", "date", "s3_key"]

# Create DataFrame from sample input data
datarecordsdf = spark.createDataFrame(data, schema=schema)

# Apply pattern matching and extraction of keywords
for keyword in keywordslist:
    datarecordsdf = datarecordsdf.withColumn(keyword, (col("data").contains(keyword)).cast("int"))
    datarecordsdf.show()
# Collect the matched keywords as an array
matched_columns = [col(keyword) for keyword in keywordslist]
print(matched_columns)
datarecordsdf = (
    datarecordsdf
    .withColumn("matchedkeywords", array([lit(keyword) for keyword in keywordslist]))
    .withColumn("matchedkeywords", array(*[col(keyword) for keyword in keywordslist]))
    .filter((col("matchedkeywords")[0] != "0") | (col("matchedkeywords")[1] != "0") | (col("matchedkeywords")[2] != "0"))
    .select("data", "date", "s3_key", "matchedkeywords")
)

datarecordsdf.show(truncate=False)

# Stop the SparkSession
spark.stop()
