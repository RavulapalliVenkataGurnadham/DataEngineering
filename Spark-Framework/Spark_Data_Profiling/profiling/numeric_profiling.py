from utils.spark_helper import get_spark_session, load_config
from pyspark.sql import functions as F
import sys
import json
def profile_numeric_data(spark, input_path, numeric_columns, output_path):

    #dealing the different file types
    file_extension = input_path.split(".")[-1].lower()
    supported_formats = ['csv', 'json', 'avro', 'tsv', 'text', 'parquet', 'orc', 'xml']
    if file_extension not in supported_formats:
        print("Unsupported file type:", file_extension)
    
    if file_extension == 'tsv':
        df = spark.read.csv(input_path, sep=r'\t', header=True, inferSchema=True)
    else:
        df = spark.read.format(file_extension).option("header", "true").load(input_path)
    # df = spark.read.csv(input_path, header=True, inferSchema=True)

    profile_report = {
        'numeric_columns': []
    }

    for column in numeric_columns:
        column_data = df.select(column)
        total_records = df.count()
        # F.expr(f'percentile_approx({column}, 0.5)').alias('median'),
        metrics = column_data.agg(
            F.countDistinct(column).alias('distinct_count'),
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias('null_count'),
            F.sum(F.when(F.col(column) == 0, 1).otherwise(0)).alias('zero_count'),
            F.mean(column).alias('mean'),
            F.min(column).alias('min'),
            F.max(column).alias('max')
        ).first()
        distinct_count = metrics['distinct_count']
        null_count = metrics['null_count']
        zero_count = metrics['zero_count']
        mean = metrics['mean']
        # medain = metrics['median']
        min = metrics['min']
        max = metrics['max']
        # summary_stats = column_data.describe().toPandas().set_index('summary').T.to_dict()
        # null_count = column_data.filter(column_data[column].isNull()).count()
        column_summary = {
            'column_name': column,
            'total_records': total_records,
            'distinct_count': distinct_count,
            'null_count': null_count,
            'zero_count': zero_count,
            'percent_missing': (null_count / total_records) * 100,
            # 'summary_statistics': summary_stats,
            'mean': mean,
            # 'median': medain,
            'min': min,
            'max': max
        }
        profile_report['numeric_columns'].append(column_summary)

    with open(output_path, 'a') as f:
        # f.write(str(profile_report))
        f.write(json.dumps(profile_report))
        f.write('\n')
        print("writing numerical data profiling report to the csv file ")

def run_numeric_profiling(config_path: str):
    config = load_config(config_path)
    spark = get_spark_session(
        config['spark']['app_name'],
        config['spark']['master'],
        config['spark']['config']
    )

    input_path = config['data']['input_path']
    numeric_columns = config['data']['numeric_columns']
    output_path = config['data']['output_path']

    profile_numeric_data(spark, input_path, numeric_columns, output_path)

# if __name__ == "__main__":
#     run_numeric_profiling(sys.argv[1])
