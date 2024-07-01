# import sys
# from profiling.numeric_profiling import run_numeric_profiling
# from profiling.categorical_profiling import run_categorical_profiling
# from frontend.data_profiling.st_categorical_profiling import display_profile_report
# import streamlit as st

# if __name__ == "__main__":
#     config_path = "config/config.yaml"
#     run_numeric_profiling(config_path)
#     run_categorical_profiling(config_path)

# if __name__ == "__main__":
#     if len(sys.argv) != 2:
#         print("Usage: python main.py <config_path>")
#         sys.exit(1)
#     config_path = sys.argv[1]
#     run_categorical_profiling(config_path)
#     run_numeric_profiling(config_path)
#     categorical_profile_report = run_categorical_profiling(config_path)
#     display_profile_report(categorical_profile_report)
# from Spark_Data_Profiling.data_sources.read_data import read_data
# import sys
# import os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from data_sources.read_data import read_data
from data_sources.data_sources import data_sources
# from utils.spark_helper import get_spark_session

def main(data_source, specific_type, profiling_level):
    # spark = get_spark_session("data_profiling_columns")
    print("main function is excuting with**********************")
    print(" datasource selcted",data_source)
    print(" datasource specific_type selcted",specific_type)
    print(" profiling level selcted",profiling_level)

    # data_source_out = data_sources(data_source='file_formats')
    data_source_out = data_sources(data_source,profiling_level)
    return data_source_out
# if __name__ == "__main__":
#     print("main ***************")
#     main()