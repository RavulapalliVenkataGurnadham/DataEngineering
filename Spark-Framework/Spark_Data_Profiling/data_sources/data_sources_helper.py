from pyspark.sql.types import NumericType, StringType, TimestampType, DateType, BooleanType
def get_column_data_type(file_df,column_name):

    if column_name not in file_df.column:
        raise ValueError(f"Column '{column_name}' not found in DataFrame")
    column_data_type = file_df.schema[column_name].dataType
    if isinstance(column_data_type, (NumericType, BooleanType)):
        return "numeric"
    elif isinstance(column_data_type, StringType):
        return "categorical"
    elif isinstance(column_data_type, (TimestampType, DateType)):
        return "datetime"
    else:
        return "unknown column type"

