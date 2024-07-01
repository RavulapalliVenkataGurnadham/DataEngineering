
from data_sources.file_formats.column_profiling import column_level_profiling
from data_sources.file_formats.dataset_profiling import dataset_level_profiling
from data_sources.file_formats.read_file_data import read_file_formats
from utils.spark_helper import load_config,get_spark_session
# reading file formats
# fetching file formats
config_file = 'Spark_Data_Profiling/config/config.yaml'
config = load_config(config_file)
input_data_path = config['data']['input_path']
spark = get_spark_session("data_profiling_columns")
def file_formats(data_profiling_level):
    print("file_formats ******************")
    # Implementation for file format
    # column_name ='order_id'
    column_name = 'Country'
    # dataset_name = 'customers_dataset'
    file_df = read_file_formats(spark,input_data_path)
    if data_profiling_level == 'column_level_profiling':
        return column_level_profiling(file_df, column_name)
    elif data_profiling_level == "dataset_level_profiling":
        return dataset_level_profiling(file_df)
    else:
        print("Please select the profiling level")
# def file_format(parms):
#     # Implementation for file format
#     # if data_profiling_level = 'structural_profiling':
#     #     structural_profiling = structural_level_profiling(params)
#     # distinct_count, null_count, value_distribution, min/max values, statistical 
#     if data_profiling_level = 'column_level_profiling':
#         column_level_profiling = column_level_profiling(params)
#     #rowcount,duplicate_count
#     elif data_profiling_level = "dataset_level_profiling":
#        dataset_level_profiling = dataset_level_profiling(params)
#     else:
#         print("please select the Profiling level")