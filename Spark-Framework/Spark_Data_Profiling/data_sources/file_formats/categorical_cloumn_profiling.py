from pyspark.sql import functions as F
from utils.spark_helper import get_spark_session,load_config
spark = get_spark_session("data_profiling_columns")
config_file = 'Spark_Data_Profiling\config\config.yaml'
config = load_config(config_file)
def categorical_column_profiling(df, column_name):

    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' is not in DataFrame")
    else:
        print("Categorical Profiling")
        column_data = df.select(column_name)
        total_records = df.count()
   
    # df = spark.read.csv(input_path, header=True, inferSchema=True)

        total_records = df.count()
    
    # profile_report = {
    #     'categorical_columns': []
    # }

    # for column in categorical_columns:
        column_data = df.select(column_name)
        
        # Aggregating all metrics
        metrics = column_data.agg(
            F.countDistinct(column_name).alias('distinct_count'),
            F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias('null_count'),
            F.sum(F.when(F.col(column_name) == 0, 1).otherwise(0)).alias('zero_count')
        ).first()

        # Fetching the mode and top values
        value_counts = column_data.groupBy(column_name).count().orderBy(F.desc('count'))
        value_counts_df = df.groupBy(column_name).count().orderBy(F.desc('count'))
        value_counts_dict = {str(row[column_name]): row['count'] for row in value_counts_df.collect()}
        # value_counts_dict = {str(row[column]): row['count'] for row in value_counts}
        mode = value_counts.first()[column_name] if value_counts.count() > 0 else None
        # top_values = value_counts.take(10)
        top_values_list = [(row[column_name], row['count']) for row in value_counts_df.take(10)]
        top_values_dict = {country: count for country, count in top_values_list}
    

        distinct_count = metrics['distinct_count']
        null_count = metrics['null_count']
        zero_count = metrics['zero_count']
        
        column_summary = {
            'column_name': column_name,
            'total_records': total_records,
            'distinct_count': distinct_count,
            'zero_count': zero_count,
            'null_count': null_count,
            'percent_missing': (null_count / total_records) * 100,
            'value_counts': value_counts_dict,
            'mode': mode,
            'top_values':  top_values_dict
        }
        
        print("column summary",column_summary)
        summary_df = spark.createDataFrame([column_summary])
        summary_df.show()
        # profile_report['categorical_columns'].append(column_summary)

    # with open(output_path, 'a') as f:
        # f.write(json.dumps(profile_report))
        # f.write('\n')
        # f.write(str(profile_report))
        # print("writing categorical data profiling report to the csv file ")
        # print("writting reporting to the csv file is Done")
    # return profile_report