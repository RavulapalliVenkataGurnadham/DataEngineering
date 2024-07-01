
# def read_file_formats(spark,filepath):
#     print("read file data")
#     format = filepath.split(".")[-1].lower()
#     supported_formats = {
#         "csv": spark.read.csv,
#         "parquet": spark.read.parquet,
#         "json": spark.read.json,
#         "orc": spark.read.orc,
#         "txt": spark.read.text
#     }

#     try:
#         read_func = supported_formats[format]
#         df = read_func(filepath).cache()
#         return df
#     except KeyError:
#         raise ValueError(f"Unsupported file format: {format}")


def read_file_formats(spark,filepath):
    print("read file data")
    format = filepath.split(".")[-1].lower()
    supported_formats = {
        "csv": lambda path: spark.read.csv(path, inferSchema=True,header= True),
        "parquet": lambda path: spark.read.parquet(path, inferSchema=True),
        "json": lambda path: spark.read.json(path, inferSchema=True),
        "orc": lambda path: spark.read.orc(path, inferSchema=True),
        "txt": lambda path: spark.read.text(path)
    }

    try:
        read_func = supported_formats[format]
        df = read_func(filepath).cache()
        return df
    except KeyError:
        raise ValueError(f"Unsupported file format: {format}")