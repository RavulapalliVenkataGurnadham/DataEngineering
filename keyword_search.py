from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, explode, split, col,collect_list,lit, when
import re
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

# Create a Spark session
spark = SparkSession.builder.appName("ParagraphSplitter").getOrCreate()

text = """ Spark is a powerful,Saprk is  open source data processing framework that's it  designed for
      speed and efficiency. It can handle both source batch and real-time data processing, making
          it a versatile tool for big data analytics.                                                                                                                                                                                                                           
One of Spark's key features is it in-memory processing capabilities, Spark which enable it to perform data operations much faster than traditional disk-based systems. This speed advantage is particularly useful for iterative algorithms and interactive data exploration."""
paragraphs = text.split('\n')
rows = [Row(paragraph=element) for element in paragraphs]
df = spark.createDataFrame(rows)
df = df.withColumn("paragraph_id", monotonically_increasing_id())
keywords = ["Spark", "it"]
def split_paragraph(paragraph, keywords):
    split_paragraphs = []
    start = 0
    for keyword in keywords:
        keyword_pos = paragraph.find(keyword, start)
        if keyword_pos != -1:
            split_paragraphs.append(paragraph[start:keyword_pos])
            start = keyword_pos + len(keyword)
    split_paragraphs.append(paragraph[start:])
    return split_paragraphs
split_paragraph_udf = udf(lambda x: split_paragraph(x, keywords), ArrayType(StringType()))

df = df.withColumn("split_paragraphs", split_paragraph_udf(df["paragraph"]))

# Explode the split paragraphs
df = df.withColumn("split_paragraph", explode(col("split_paragraphs")))

# Select only non-empty split paragraphs
df = df.filter(col("split_paragraph") != "")
df = df.drop("split_paragraphs")
df.show()