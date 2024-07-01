from pyspark.sql import * 
if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Helooo Spark")\
        .master("local[2]")\
        .getOrCreate()
    data_list = [("Ravi", 23), ("David", 24)]
    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()