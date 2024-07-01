from data_sources.read_data import read_file_formats
# from data_sources.data_sources_helper import get_column_data_type
from utils.spark_helper import get_spark_session, load_config
spark = get_spark_session("data_profiling_columns")
config_file = 'Spark_Data_Profiling/config/config.yaml'
config = load_config(config_file)
input_data_path = config['data']['input_path']
file_df = read_file_formats(spark,input_data_path)

def dataset_level_profiling(file_df):
    print("dataset level profiling started")
    total_records = file_df.count()
    columns_list = file_df.columns
    # total_no_columns = file_df.columns.size
    total_no_columns = len(file_df.columns)
    columns_data_types = file_df.dtypes
    non_duplicate_file_df = file_df.dropDuplicates()
    duplicate_count = file_df.exceptAll(non_duplicate_file_df).count()
    duplicate_percentage = (duplicate_count / total_records) * 100
    column_summary = {
        'total_records': total_records,
        'column_list': columns_list,
        'total_no_columns': total_no_columns,
        'columns_data_types': columns_data_types,
        'duplicate_count': duplicate_count,
        'duplicate_percentage': duplicate_percentage
    }
    print("column summary",column_summary)
    # summary_df = spark.createDataFrame([column_summary])
    print("dataset level profiling completed")
    # summary_df.show()
    return column_summary