# all imports
from data_sources.data_lakes.data_lakes import data_lakes;
from data_sources.data_warehouse.data_warehouse import data_warehouse
from data_sources.databases.databases import databases
from data_sources.file_formats.file_formats import file_formats

def data_sources(data_source,profiling_level):
    data_source_map = {
        "databases": databases,
        "file_formats": file_formats,
        "datawarehouse": data_warehouse,
        "datalakes": data_lakes
    }
    
    # get the  function based on the data_source value
    selected_source = data_source_map.get(data_source)
    # data_profiling_level = 'column_level_profiling'
    # data_profiling_level = "dataset_level_profiling"
    # data_profiling_level = profiling_level
    data_profiling_level = profiling_level + '_level_profiling'
    if selected_source:
        print(f"data source execution started for {selected_source}")
        return selected_source(data_profiling_level)
    else:
        print("Please select the correct data source")
