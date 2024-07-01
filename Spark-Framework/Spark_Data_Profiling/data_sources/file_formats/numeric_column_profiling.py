import datetime
from pyspark.sql import functions as F
from utils.spark_helper import get_spark_session,load_config
spark = get_spark_session("data_profiling_columns")
config_file = 'Spark_Data_Profiling\config\config.yaml'
config = load_config(config_file)
# output_data_path = config['data']['output_path']
def numeric_column_profiling(df, column_name):
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' is not in DataFrame")
    else:
        print("Numerical Profiling")
        column_data = df.select(column_name)
        total_records = df.count()

        metrics = column_data.agg(
            F.countDistinct(column_name).alias('distinct_count'),
            F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias('null_count'),
            F.sum(F.when(F.col(column_name) == 0, 1).otherwise(0)).alias('zero_count'),
            F.mean(column_name).alias('mean'),
            F.min(column_name).alias('min'),
            F.max(column_name).alias('max')
        ).first()

        distinct_count = metrics['distinct_count']
        null_count = metrics['null_count']
        zero_count = metrics['zero_count']
        mean = metrics['mean']
        min_val = metrics['min']
        max_val = metrics['max']

        column_summary = {
            'total_records': total_records,
            'distinct_count': distinct_count,
            'null_count': null_count,
            'zero_count': zero_count,
            'percent_missing': (null_count / total_records) * 100,
            'mean': mean,
            'min': min_val,
            'max': max_val
        }
        print("column summary",column_summary)
        summary_df = spark.createDataFrame([column_summary])
        summary_df.show()

        # Write the DataFrame  to a CSV file
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # output_file_name = f"{column_name}_{timestamp}.csv"
        # output_file_path = f"{output_data_path}/{output_file_name}"
        output_data_path = "Spark_Data_Profiling/data/out.csv"
        summary_df.write.csv(output_data_path)

        # summary_df.write.csv(output_data_path, header=True, mode="overwrite")

        print("sending the column profiling to the csv file is done ")
        return {column_name: column_summary}

