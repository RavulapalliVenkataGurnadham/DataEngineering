from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import monotonically_increasing_id, explode, col
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import re
spark = SparkSession.builder.appName("ParagraphSplitter").getOrCreate()
text = """ Spark is a powerful, Spark is open source data processing framework that's it designed for speed and efficiency. 
It can handle both source batch and real-time data processing, making 
it a versatile tool for big data analytics. 
One of Spark's key features is its in-memory processing capabilities, Spark which enable it to perform data operations much faster than traditional it  systems. This speed advantage is particularly useful for iterative algorithms and interactive data exploration."""
paragraphs = text.split('\n')
rows = [Row(paragraph=element) for element in paragraphs]
df = spark.createDataFrame(rows)
# df = df.withColumn("paragraph_id", monotonically_increasing_id())
keywords = ["Spark", "it", "is", "and"]
def split_paragraph(paragraph, keywords):
    split_paragraphs = []
    start = 0
    for keyword in keywords:
        # keyword_pos = paragraph.find(keyword, start)
        keyword_index = re.search(r'\b' + re.escape(keyword) + r'\b', paragraph[start:])
        if keyword_index:
            keyword_pos = keyword_index.start() + start
            if keyword_pos != -1:
                split_paragraphs.append(paragraph[start:keyword_pos])
                start = keyword_pos + len(keyword) 
    split_paragraphs.append(paragraph[start:])
    return split_paragraphs
split_paragraph_udf = udf(lambda x: split_paragraph(x, keywords), ArrayType(StringType()))
df1 = df.withColumn("split_paragraphs", split_paragraph_udf(df["paragraph"]))
df1.drop('paragraph')
df2 = df1.withColumn("paragraph", explode(col("split_paragraphs")))
df2 = df2.filter(col("paragraph") != "")
df2 = df2.drop("split_paragraphs")
df2 = df2.withColumn("paragraph_id", monotonically_increasing_id())
df2.show()
