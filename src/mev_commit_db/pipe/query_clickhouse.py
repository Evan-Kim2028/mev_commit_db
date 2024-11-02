import logging
import polars as pl
import pyarrow as pa
from clickhouse_connect.driver import Client

# Configure logging for this module
logger = logging.getLogger(__name__)

arrow_to_clickhouse_type_map = {
    pa.int8(): "Int8",
    pa.int16(): "Int16",
    pa.int32(): "Int32",
    pa.int64(): "Int64",
    pa.uint8(): "UInt8",
    pa.uint16(): "UInt16",
    pa.uint32(): "UInt32",
    pa.uint64(): "UInt64",
    pa.float32(): "Float32",
    pa.float64(): "Float64",
    pa.string(): "String",
    pa.binary(): "FixedString",
    pa.bool_(): "Bool",
    pa.timestamp("s"): "DateTime",
    pa.timestamp("ms"): "DateTime64(3)",  # millisecond precision
    pa.timestamp("us"): "DateTime64(6)",  # microsecond precision
    pa.timestamp("ns"): "DateTime64(9)",  # nanosecond precision
    pa.date32(): "Date32",
    pa.date64(): "Date",
    pa.list_(pa.int32()): "Array(Int32)",  # Example for list types, adjust as needed
    pa.list_(pa.float64()): "Array(Float64)",
    pa.struct(
        [("field1", pa.int32()), ("field2", pa.string())]
    ): "Tuple(Int32, String)",  # Example, adjust per schema
    pa.dictionary(
        index_type=pa.int8(), value_type=pa.string()
    ): "LowCardinality(String)",
}


# Example function to convert a PyArrow schema to ClickHouse types
def map_arrow_schema_to_clickhouse(schema: pa.schema) -> dict[str, str]:
    clickhouse_schema = {}
    for field in schema:
        arrow_type = field.type
        clickhouse_type = arrow_to_clickhouse_type_map.get(
            arrow_type, "String"
        )  # Default to String if no match found
        clickhouse_schema[field.name] = clickhouse_type
    return clickhouse_schema


def create_table_if_not_exists(client: Client, table_name: str, schema: pa.Schema):
    """
    Create a table in ClickHouse if it does not already exist based on the given Arrow schema.
    """
    # Map Arrow schema to ClickHouse schema
    clickhouse_schema = map_arrow_schema_to_clickhouse(schema)

    # Format columns as "name type" for each column in the schema
    columns = ", ".join(
        [f"{name} {dtype}" for name, dtype in clickhouse_schema.items()]
    )

    # Define the CREATE TABLE query
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}) ENGINE = MergeTree() ORDER BY tuple()"

    # Check if the table exists
    table_exists_query = f"EXISTS TABLE {table_name}"
    exists = client.command(table_exists_query)

    if exists:
        # Log if the table already exists
        logger.info(
            f"Table '{table_name}' already exists with schema: {clickhouse_schema}"
        )
    else:
        # Create the table and log the creation
        client.command(create_query)
        logger.info(f"Created table '{table_name}' with schema: {clickhouse_schema}")


def write_to_clickhouse(client: Client, table_name: str, df: pl.DataFrame):
    """
    Write a Polars DataFrame to ClickHouse using JSON format for efficiency.
    """
    if df.is_empty():
        logger.warning(f"No data to insert for table {table_name}")
        return

    # Convert Polars DataFrame to Arrow Table
    arrow_table = df.to_arrow()
    create_table_if_not_exists(client, table_name, arrow_table.schema)

    # Insert JSON data into ClickHouse
    client.insert_arrow(table=table_name, arrow_table=arrow_table)
    logger.info(f"Inserted data into table: {table_name}")


# import asyncio
# import os
# import polars as pl
# import logging
# import traceback
# from typing import Any, Dict, List, Union, Optional
# from hypermanager.manager import HyperManager
# from hypermanager.protocols.mev_commit import mev_commit_config
# from sqlalchemy import create_engine, MetaData, Table, Column
# from clickhouse_sqlalchemy import get_declarative_base, types, engines
# from sqlalchemy.exc import SQLAlchemyError

# # Database configurations
# CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
# CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "9000")
# CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
# CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
# CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "mev_commit_testnet")

# # Set up SQLAlchemy engine and session with types_check properly set
# SQLALCHEMY_DATABASE_URL = (
#     f"clickhouse+native://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@"
#     f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
# )

# engine = create_engine(
#     SQLALCHEMY_DATABASE_URL, connect_args={"settings": {"types_check": True}}
# )

# Base = get_declarative_base()
# metadata = MetaData()

# # Configure logging with INFO level
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[logging.StreamHandler()],
# )
# logger = logging.getLogger(__name__)


# def prepare_table_configs(event_configs: List[str]) -> list[dict[str]]:
#     tables = []
#     for event_name in event_configs:
#         table = {
#             "table_name": event_name.lower(),
#             "event_config": mev_commit_config[event_name],
#         }
#         if event_name == "OpenedCommitmentStored":
#             table["special_handling"] = "OpenedCommitmentStored"
#         tables.append(table)
#     return tables


# async def fetch_l1_txs(l1_tx_list: Union[str, list[str]]) -> Optional[pl.DataFrame]:
#     if not l1_tx_list:
#         logger.info("No L1 transaction hashes to query.")
#         return None
#     if isinstance(l1_tx_list, str):
#         l1_tx_list = [l1_tx_list]
#     manager = HyperManager(url="https://holesky.hypersync.xyz")
#     dataframes = []

#     def chunked(iterable, n):
#         for i in range(0, len(iterable), n):
#             yield iterable[i : i + n]

#     for chunk in chunked(l1_tx_list, 1000):
#         try:
#             l1_txs_chunk = await manager.search_txs(txs=chunk)
#             if l1_txs_chunk is not None and not l1_txs_chunk.is_empty():
#                 dataframes.append(l1_txs_chunk)
#         except Exception as e:
#             logger.error(
#                 f"Unexpected error while fetching L1 transactions for chunk: {e}"
#             )
#             continue
#     if not dataframes:
#         logger.info("No L1 transactions found.")
#         return None
#     return pl.concat(dataframes)


# def map_and_cast_schema(df: pl.DataFrame) -> pl.DataFrame:
#     # Define specific column type mapping
#     specific_column_types = {
#         "base_fee_per_gas": pl.Float64,
#         "effective_gas_price": pl.Float64,
#         "gas_used": pl.Float64,
#         # Add other columns that should be Float64
#     }

#     # First, apply specific column type casts
#     for col, target_type in specific_column_types.items():
#         if col in df.columns:
#             df = df.with_columns(
#                 pl.col(col).cast(target_type, strict=False).fill_null(0)
#             )

#     # Now, define the dynamic schema mapping
#     schema_mapping = {
#         pl.Int64: pl.Int64,
#         pl.Float64: pl.Float64,
#         pl.Float32: pl.Float32,
#         pl.Utf8: pl.Utf8,
#         pl.Boolean: pl.UInt8,
#         pl.UInt64: pl.UInt64,
#         pl.UInt8: pl.UInt8,
#     }

#     # Apply casting dynamically for all columns based on inferred types
#     for col, dtype in zip(df.columns, df.dtypes):
#         if col in specific_column_types:
#             continue  # Already handled
#         target_type = schema_mapping.get(dtype, pl.Utf8)  # Default to String if unknown
#         df = df.with_columns(pl.col(col).cast(target_type, strict=False))

#     return df


# def write_to_clickhouse(df: pl.DataFrame, table_name: str):
#     # Define mapping from Polars data types to ClickHouse SQLAlchemy types
#     polars_to_clickhouse_types = {
#         pl.Int8: types.Int8,
#         pl.Int16: types.Int16,
#         pl.Int32: types.Int32,
#         pl.Int64: types.Int64,
#         pl.UInt8: types.UInt8,
#         pl.UInt16: types.UInt16,
#         pl.UInt32: types.UInt32,
#         pl.UInt64: types.UInt64,
#         pl.Float32: types.Float32,
#         pl.Float64: types.Float64,
#         pl.Boolean: types.UInt8,  # ClickHouse uses UInt8 for Boolean
#         pl.Utf8: types.String,
#         pl.Date: types.Date,
#         pl.Datetime: types.DateTime,
#         # Add other mappings if necessary
#     }

#     columns = []
#     for name, dtype in df.schema.items():
#         # Map Polars data types to ClickHouse types
#         clickhouse_type = polars_to_clickhouse_types.get(dtype, types.String)
#         # Check if the column contains nulls
#         has_nulls = df.select(pl.col(name).is_null().any()).item()
#         if has_nulls:
#             # Use Nullable type
#             clickhouse_type = types.Nullable(clickhouse_type)
#         columns.append(Column(name, clickhouse_type))

#     # Fill nulls in String columns with empty strings
#     string_columns = [name for name, dtype in df.schema.items() if dtype == pl.Utf8]
#     if string_columns:
#         df = df.with_columns([pl.col(col).fill_null("") for col in string_columns])

#     # Optionally, fill nulls in Boolean columns with False (0)
#     bool_columns = [name for name, dtype in df.schema.items() if dtype == pl.Boolean]
#     if bool_columns:
#         df = df.with_columns([pl.col(col).fill_null(False) for col in bool_columns])

#     with engine.connect() as connection:
#         # Check and create table if it doesn't exist
#         if not engine.dialect.has_table(connection, table_name):
#             # Define columns dynamically based on DataFrame schema
#             columns = []
#             for name, dtype in df.schema.items():
#                 # Map Polars data types to ClickHouse types
#                 clickhouse_type = polars_to_clickhouse_types.get(dtype, types.String)
#                 columns.append(Column(name, clickhouse_type))
#             # Create the table with the dynamically defined columns
#             table = Table(
#                 table_name,
#                 metadata,
#                 *columns,
#                 engines.MergeTree(
#                     order_by=[df.columns[0]]
#                 ),  # Adjust order_by as needed
#             )
#             table.create(bind=engine)

#         # Insert data into the table
#         try:
#             df.write_database(
#                 table_name=f"{CLICKHOUSE_DB}.{table_name}",
#                 connection=connection,
#                 if_table_exists="append",
#             )
#         except Exception as e:
#             logger.error(f"Error inserting data: {e}")
#             traceback.print_exc()


# def get_last_processed_block(table_name: str) -> int:
#     """
#     Returns either 0 or the latest block_number from the table.
#     """
#     with engine.connect() as connection:
#         if not engine.dialect.has_table(connection, table_name):
#             logger.debug(f"Table '{table_name}' does not exist. Starting from block 0.")
#             return 0
#         else:
#             try:
#                 # Query the maximum block number
#                 query = f"SELECT MAX(block_number) AS last_block FROM {table_name}"
#                 df = pl.read_database(query=query, connection=engine)

#                 if not df.is_empty() and df[0, "last_block"] is not None:
#                     # Convert to int, defaulting to 0 if conversion fails
#                     try:
#                         return int(df[0, "last_block"]) + 1
#                     except ValueError:
#                         logger.warning(
#                             f"No valid block_number found in '{table_name}'. Starting from block 0."
#                         )
#                         return 0
#                 else:
#                     logger.info(
#                         f"Table '{table_name}' exists but has no data. Starting from block 0."
#                     )
#                     return 0
#             except Exception as e:
#                 logger.error(
#                     f"Unexpected error retrieving last block from '{table_name}': {e}"
#                 )
#                 traceback.print_exc()
#                 return 0


# async def get_events():
#     manager = HyperManager(url="https://mev-commit.hypersync.xyz")
#     event_names = [event_name for event_name in mev_commit_config.keys()]
#     tables: List[Dict[str, Any]] = prepare_table_configs(event_names)
#     for table in tables:
#         from_block = get_last_processed_block(table["table_name"])
#         logger.info(f"Querying {table['table_name']} from block {from_block}")
#         try:
#             df: pl.DataFrame = await manager.execute_event_query(
#                 table["event_config"],
#                 tx_data=True,
#                 from_block=from_block,
#             )
#             record_count = len(df)
#             if record_count > 0:
#                 try:
#                     write_to_clickhouse(df, table["table_name"])
#                     logger.info(
#                         f"{table['table_name']}: {record_count} new records written."
#                     )
#                 except Exception as e:
#                     logger.error(
#                         f"Error writing to ClickHouse for {table['table_name']}: {e}"
#                     )
#                     traceback.print_exc()
#             else:
#                 logger.info(f"{table['table_name']}: 0 new records.")

#             if (
#                 table.get("special_handling") == "OpenedCommitmentStored"
#                 and not df.is_empty()
#             ):
#                 l1_txs_list = (
#                     df.with_columns((pl.lit("0x") + pl.col("txnHash")).alias("txnHash"))
#                     .select("txnHash")
#                     .unique()["txnHash"]
#                     .to_list()
#                 )
#                 l1_txs_df = await fetch_l1_txs(l1_txs_list)
#                 if l1_txs_df is not None and not l1_txs_df.is_empty():
#                     write_to_clickhouse(l1_txs_df, "l1_transactions")
#                     logger.info(
#                         f"l1_transactions: {len(l1_txs_df)} new records written."
#                     )
#                 else:
#                     logger.info("l1_transactions: 0 new records.")
#         except ValueError as e:
#             logger.error(f"Error querying {table['table_name']}: {e}")
#             # traceback.print_exc() # only needed for advanced debugging
#             continue


# if __name__ == "__main__":

#     async def main_loop():
#         while True:
#             try:
#                 await get_events()
#             except Exception as e:
#                 logger.error(f"Error in get_events: {e}")
#                 traceback.print_exc()
#             await asyncio.sleep(30)

#     asyncio.run(main_loop())
