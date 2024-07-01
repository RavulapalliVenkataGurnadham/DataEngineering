from utils.spark_helper import get_spark_session, load_config
import sys
import json
from pyspark.sql import functions as F

def profile_categorical_data(spark, input_path, categorical_columns, output_path):
    
    #dealing different file types
    file_extension = input_path.split(".")[-1].lower()
    supported_formats = ['csv', 'json', 'avro', 'tsv', 'text', 'parquet', 'orc', 'xml']
    if file_extension not in supported_formats:
        print("Unsupported file type:", file_extension)
        return
    
    if file_extension == 'tsv':
        df = spark.read.csv(input_path, sep=r'\t', header=True, inferSchema=True)
    else:
        df = spark.read.format(file_extension).option("header", "true").load(input_path)
    # df = spark.read.csv(input_path, header=True, inferSchema=True)

    total_records = df.count()
    
    profile_report = {
        'categorical_columns': []
    }

    for column in categorical_columns:
        column_data = df.select(column)
        
        # Aggregating all metrics
        metrics = column_data.agg(
            F.countDistinct(column).alias('distinct_count'),
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias('null_count'),
            F.sum(F.when(F.col(column) == 0, 1).otherwise(0)).alias('zero_count')
        ).first()

        # Fetching the mode and top values
        value_counts = column_data.groupBy(column).count().orderBy(F.desc('count'))
        value_counts_df = df.groupBy(column).count().orderBy(F.desc('count'))
        value_counts_dict = {str(row[column]): row['count'] for row in value_counts_df.collect()}
        # value_counts_dict = {str(row[column]): row['count'] for row in value_counts}
        mode = value_counts.first()[column] if value_counts.count() > 0 else None
        # top_values = value_counts.take(10)
        top_values_list = [(row[column], row['count']) for row in value_counts_df.take(10)]
        top_values_dict = {country: count for country, count in top_values_list}
    

        distinct_count = metrics['distinct_count']
        null_count = metrics['null_count']
        zero_count = metrics['zero_count']
        
        column_summary = {
            'column_name': column,
            'total_records': total_records,
            'distinct_count': distinct_count,
            'zero_count': zero_count,
            'null_count': null_count,
            'percent_missing': (null_count / total_records) * 100,
            'value_counts': value_counts_dict,
            'mode': mode,
            'top_values':  top_values_dict
        }
        
        profile_report['categorical_columns'].append(column_summary)

    with open(output_path, 'a') as f:
        f.write(json.dumps(profile_report))
        f.write('\n')
        # f.write(str(profile_report))
        print("writing categorical data profiling report to the csv file ")
        print("writting reporting to the csv file is Done")
    return profile_report
def run_categorical_profiling(config_path: str):
    config = load_config(config_path)
    spark = get_spark_session(
        config['spark']['app_name'],
        config['spark']['master'],
        config['spark']['config']
    )

    input_path = config['data']['input_path']
    categorical_columns = config['data']['categorical_columns']
    output_path = config['data']['output_path']
    # input_path = r'data\customers-100000.csv'
    # output_path = r'data\output_customers-100000.csv'
    # categorical_columns = ['Country','Website']


    profile_categorical_data(spark, input_path, categorical_columns, output_path)  






