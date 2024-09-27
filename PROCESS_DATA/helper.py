from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO

from PROCESS_DATA.models import DataMinionPathInfo, TableInfoRequest, InsertRequest
from common.CommonUtils import CommonUtils
from common.SqlAlchemyUtil import SqlAlchemyUtil
from common.MinioUtils import MinioUtils
from common.VaultUtils import VaultUtils
from constants import IOT_BUCKET_NAME, IOT_SCHEMA, HIVE_CATALOG, TRINO_DATA_TYPE_MAPPING, \
    YEAR_STR, MONTH_STR, DAY_STR, HOUR_STR
import pyarrow.parquet as pq
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.INFO)

minio_util = MinioUtils().get_instance()
vault_utils = VaultUtils()

secret_data = vault_utils.read_secret(path='iot-trino')
TRINO_CONNECTION_STRING = secret_data['trino_connection_string']


def get_data_from_parquet(request: DataMinionPathInfo):
    table_name = request.table_name
    year_path = f'/{YEAR_STR}={request.year}' if request.year is not None else ''
    month_path = f'/{MONTH_STR}={request.month}' if request.month is not None else ''
    day_path = f'/{DAY_STR}={request.day}' if request.day is not None else ''
    hour_path = f'/{HOUR_STR}={request.hour}' if request.hour is not None else ''
    path = f'{table_name}{year_path}{month_path}{day_path}{hour_path}/'
    recursive = request.hour is None

    list_file_path = minio_util.get_list_files(IOT_BUCKET_NAME, path, recursive)

    if not list_file_path or len(list_file_path) == 0:
        return None

    return combine_parquet_files(IOT_BUCKET_NAME, list_file_path)


def create_external_table(request: TableInfoRequest):
    # generate query create external table
    column_list = [f"{column.column_name} {TRINO_DATA_TYPE_MAPPING[column.type]}" for column in request.columns]
    column_list.append(f"{YEAR_STR} {TRINO_DATA_TYPE_MAPPING['int']}")
    column_list.append(f"{MONTH_STR} {TRINO_DATA_TYPE_MAPPING['int']}")
    column_list.append(f"{DAY_STR} {TRINO_DATA_TYPE_MAPPING['int']}")
    column_list.append(f"{HOUR_STR} {TRINO_DATA_TYPE_MAPPING['int']}")
    columns_str = ',\n'.join(column_list)

    sql_str = f"""
        CREATE TABLE IF NOT EXISTS {HIVE_CATALOG}.{IOT_SCHEMA}.{request.table_name} (
            {columns_str}
        ) 
        WITH(            
            external_location = 's3a://{IOT_BUCKET_NAME}/{request.table_name}/',
            partitioned_by = array ['{YEAR_STR}', '{MONTH_STR}', '{DAY_STR}', '{HOUR_STR}'],
            format = 'PARQUET'
        )
        """

    sql_alchemy_util = SqlAlchemyUtil(TRINO_CONNECTION_STRING)
    try:
        sql_alchemy_util.execute_query(sql_str)
        return "Table was created successfully"
    except Exception as e:
        logger.error(f'Error when create new table: {str(e)}')
        raise Exception("An error occurred while executing the query. Please check info of table")


def insert_data(request: InsertRequest):
    # Get the current date and time
    now = datetime.now()

    # Extract year, month, day, and hour
    current_year = str(now.year)
    current_month = str(now.month)
    current_day = str(now.day)
    current_hour = str(now.hour)
    list_insert_query = []
    for data in request.values:
        column_names = [f"{column.column_name}" for column in data]
        column_names.extend([YEAR_STR, MONTH_STR, DAY_STR, HOUR_STR])
        column_values = [f"{column.column_value}" for column in data]
        column_values.extend([current_year, current_month, current_day, current_hour])

        column_types = [f"{TRINO_DATA_TYPE_MAPPING[column.column_type]}" for column in data]
        column_types.extend([TRINO_DATA_TYPE_MAPPING['int'], TRINO_DATA_TYPE_MAPPING['int'],
                             TRINO_DATA_TYPE_MAPPING['int'], TRINO_DATA_TYPE_MAPPING['int']])

        formatted_column_values = []
        for value, column_type in zip(column_values, column_types):
            if column_type != "VARCHAR":
                formatted_column_values.append(str(value))
            else:
                formatted_column_values.append(f"'{value}'")

        column_names_str = ", ".join(column_names)
        column_values_str = ", ".join(formatted_column_values)

        if column_types == "VARCHAR":
            insert_row_query = f"""
                INSERT INTO {HIVE_CATALOG}.{IOT_SCHEMA}.{request.table_name} ({column_names_str}) VALUES ('{column_values_str}')
                """
        else:
            insert_row_query = f"""
                INSERT INTO {HIVE_CATALOG}.{IOT_SCHEMA}.{request.table_name} ({column_names_str}) VALUES ({column_values_str})
            """
        list_insert_query.append(insert_row_query)

    sql_alchemy_util = SqlAlchemyUtil(TRINO_CONNECTION_STRING)
    try:
        sql_alchemy_util.execute_multiple_queries(list_insert_query)
        return "Inserted data successfully"
    except Exception as e:
        logger.error(f'Error when insert data: {str(e)}')
        raise Exception(
            "An error occurred while executing the query. Please check the column name, type, and value of the columns.")


def process_parquet_file(bucket_name, file_name):
    try:
        # Download the file as a BytesIO stream
        file_data = BytesIO(minio_util.get_object(bucket_name, file_name).read())
        # Check file data is parquet file
        if not CommonUtils.is_parquet_file(file_data):
            # Return empty DataFrame on error
            return pd.DataFrame()

        # Read parquet file into a DataFrame
        table = pq.read_table(file_data)
        df = table.to_pandas()
        return df

    except Exception as e:
        logger.error(f"Error reading or processing {file_name}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error


def combine_parquet_files(bucket_name, file_list):
    dfs = []
    with ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(process_parquet_file, bucket_name, file_name): file_name for file_name in
                          file_list}
        for future in as_completed(future_to_file):
            df = future.result()
            if not df.empty:
                dfs.append(df)

    # Combine all DataFrames into one
    combined_df = pd.concat(dfs, ignore_index=True)
    # Convert DataFrame to JSON
    return combined_df.to_dict(orient='records')
