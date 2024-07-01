# # Main Streamlit app
import streamlit as st
import sys
import os
a = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..'))
sys.path.append(a)
from main import main

st.title("Data Source Selector")

# Step 1: Select Data Source
data_source = st.selectbox(
    "Select Data Source",
    ("file_formats", "datawarehouse", "databases", "datalakes")
)

# Initialize specific_type variable
specific_type = None

# Conditional blocks to handle different data sources
if data_source == "file_formats":
    specific_type = st.selectbox(
        "Select File Type",
        ("Txt", "Csv", "Excel", "Json", "Parquet", "Avro", "Orc")
    )
    uploaded_file = st.file_uploader("Upload a file", type=[specific_type.lower()])
elif data_source == "datawarehouse":
    specific_type = st.selectbox("Select Datawarehouse Type", ("Amazon_Redshift", "Google_BigQuery", "Snowflake", "Azure_Synapse_Analytics", "Hive"))
elif data_source == "databases":
    specific_type = st.selectbox("Select Database Type", ("Mysql", "Postgres", "Sqlserver", "Oracle", "Db2", "Sqlite", "Mariadb", "Mongodb", "Hbase", "Cassandra", "Redis", "Dynamodb", "Couchbase", "Neo4j", "Amazonrds", "Azuresql", "Google_cloud_sql", "Cloud_spanner", "Memcached"))
elif data_source == "datalakes":
    specific_type = st.selectbox("Select Datalake Type", ("Amazons3", "Azure_data_lake", "GCS", "BigQuery", "Delta_lake", "HDFS", "Informatica_edl", "IBM_cloud_object_storage", "Oracle_cloud_datalake"))

# Step 2: Select Profiling Level
profiling_level = st.selectbox(
    "Select Profiling Level",
    ("dataset", "column")
)

# Button to trigger main function
if st.button("Run Data Profiling"):
    if specific_type:

        print("running main function ")
        result = main(data_source, specific_type, profiling_level)
        st.write(result)
    else:
        st.write("Please select a specific type.")



# import streamlit as st
# st.title("Data Source Selector")

# # Step 1: Select Data Source
# data_source = st.selectbox(
#     "Select Data Source",
#     ("Fileformat", "Datawarehouse", "Database", "Datalake")
# )
# if data_source == "Fileformat":
#     file_type = st.selectbox(
#         "Select File Type",
#         ("Txt","Csv", "Excel", "Json", "Parquet","Avro","Orc")
#     )
#     uploaded_file = st.file_uploader("Upload a file", type=[file_type.lower()])
# elif data_source == "Datawarehouse":
#     datawarehouse_type = st.selectbox("Select Datawarehouse Type", ("Amazon_Redshift","Google_BigQuery","Snowflake","Azure_Synapse_Analytics","Hive"))
# elif data_source == "Database":
#     database_type = st.selectbox("Select Database Type", ("Mysql","Postgres","Sqlserver","Oracle","Db2","Sqlite","Mariadb","Mongodb","Hbase","Casandra","Redis","Dynamodb","Couchbase","Neo4j","Amazonrds","Azuresql","Google_cloud_sql","Cloud_spanner","Memcached",""))
# elif data_source == "Datalake":
#     datalake_type = st.selectbox("",("Amazons3","Azure_data_lake","GCS","BigQuery","Delta_lake","HDFS","Informatica_edl","IBM_cloud_object_storage","Oracle_cloud_datalake",""))

# profiling_level = st.selectbox(
#     "Select Profiling Level",
#     ("Dataset", "Column")
# )
# if st.button("Run Data Profiling"):
#     result = main(data_source, specific_type, profiling_level)
# Step 3: Display Columns and Data
# data = None
# if data_source == "File Format" and uploaded_file is not None:
#     data = get_data(data_source, file_type, uploaded_file)
# elif data_source in ["Data Warehouse", "Database"]:
#     data = get_data(data_source)

# if data is not None:
#     st.write("Data Preview:")
#     st.write(data.head())

# import streamlit as st

# st.title("Data Source Selector")

# # Step 1: Select Data Source
# data_sources = {
#     "Fileformat": ["Txt", "Csv", "Excel", "Json", "Parquet", "Avro", "Orc"],
#     "Datawarehouse": ["Amazon_Redshift", "Google_BigQuery", "Snowflake", "Azure_Synapse_Analytics", "Hive"],
#     "Database": ["Mysql", "Postgres", "Sqlserver", "Oracle", "Db2", "Sqlite", "Mariadb", "Mongodb", "Hbase", "Cassandra", "Redis", "Dynamodb", "Couchbase", "Neo4j", "Amazonrds", "Azuresql", "Google_cloud_sql", "Cloud_spanner", "Memcached"],
#     "Datalake": ["Amazons3", "Azure_data_lake", "GCS", "BigQuery", "Delta_lake", "HDFS", "Informatica_edl", "IBM_cloud_object_storage", "Oracle_cloud_datalake"]
# }

# data_source = st.selectbox("Select Data Source", list(data_sources.keys()))

# if data_source == "Fileformat":
#     file_type = st.selectbox("Select File Type", data_sources["Fileformat"])
#     uploaded_file = st.file_uploader("Upload a file", type=[file_type.lower()])
# else:
#     specific_type = st.selectbox(f"Select {data_source} Type", data_sources[data_source])

# # Further processing can be added here based on the selected options
# profiling_level = st.selectbox(
#     "Select Profiling Level",
#     ("Dataset", "Column")
# )



#     columns = data.columns.tolist()
#     selected_columns = st.multiselect("Select Columns to Display", columns)
#     if selected_columns:
#         st.write("Selected Columns:")
#         st.write(data[selected_columns].head())

