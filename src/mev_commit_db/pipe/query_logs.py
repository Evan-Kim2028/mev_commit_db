import asyncio
import logging
import os
from typing import Any, Dict, List, Union, Optional
import pandas as pd
import polars as pl
import clickhouse_connect
from hypermanager.manager import HyperManager
from hypermanager.protocols.mev_commit import mev_commit_config

# Fetch environment variables
CLICKHOUSE_HOST = os.getenv(
    "CLICKHOUSE_HOST", "db"
)  # Default to "db" as per docker-compose
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=int(CLICKHOUSE_PORT),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

event_names = [event_name for event_name in mev_commit_config.keys()]


def prepare_table_configs(event_configs: List[str]) -> list:
    tables = []
    for event_name in event_configs:
        table = {
            "table_name": event_name.lower(),
            "block_column": "block_number",
            "event_config": mev_commit_config[event_name],
        }
        if event_name == "OpenedCommitmentStored":
            table["special_handling"] = "commit_stores"
        tables.append(table)
    return tables


def polars_dtype_to_clickhouse(dtype):
    if dtype == pl.Int64:
        return "Int64"
    elif dtype == pl.Int32:
        return "Int32"
    elif dtype == pl.Int16:
        return "Int16"
    elif dtype == pl.Float64:
        return "Float64"
    elif dtype == pl.Float32:
        return "Float32"
    elif dtype == pl.Boolean:
        return "UInt8"
    elif dtype == pl.Utf8:
        return "String"
    elif dtype == pl.Datetime:
        return "DateTime"
    else:
        return "String"  # Default to String if type is unknown


def pandas_dtype_to_clickhouse(dtype):
    if pd.api.types.is_int64_dtype(dtype):
        return "Int64"
    elif pd.api.types.is_int32_dtype(dtype):
        return "Int32"
    elif pd.api.types.is_int16_dtype(dtype):
        return "Int16"
    elif pd.api.types.is_float64_dtype(dtype):
        return "Float64"
    elif pd.api.types.is_float32_dtype(dtype):
        return "Float32"
    elif pd.api.types.is_bool_dtype(dtype):
        return "UInt8"
    elif pd.api.types.is_string_dtype(dtype):
        return "String"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DateTime"
    else:
        return "String"  # Default to String if type is unknown


def generate_create_table_query(df, table_name: str) -> str:
    columns = []
    if isinstance(df, pl.DataFrame):
        df_columns = df.columns
        df_dtypes = df.dtypes
        for col_name, dtype in zip(df_columns, df_dtypes):
            ch_type = polars_dtype_to_clickhouse(dtype)
            columns.append(f"`{col_name}` {ch_type}")
    elif isinstance(df, pd.DataFrame):
        for col_name, dtype in df.dtypes.items():
            ch_type = pandas_dtype_to_clickhouse(dtype)
            columns.append(f"`{col_name}` {ch_type}")
    else:
        raise TypeError("DataFrame must be a Polars or Pandas DataFrame")

    columns_str = ", ".join(columns)

    # Use block_number as the primary ordering key for efficient querying
    query = (
        f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str}) "
        f"ENGINE = MergeTree ORDER BY block_number"
    )
    return query


def write_to_clickhouse(df, table_name: str, client):
    # Ensure the DataFrame is either Polars or Pandas
    if isinstance(df, pl.DataFrame):
        # Convert Polars DataFrame to Pandas DataFrame before inserting into ClickHouse
        df_pandas = df.to_pandas()
        logging.info(
            "Converted Polars DataFrame to Pandas DataFrame for ClickHouse insertion."
        )
    elif isinstance(df, pd.DataFrame):
        df_pandas = df
    else:
        raise TypeError("DataFrame must be a Polars or Pandas DataFrame")

    # Generate the CREATE TABLE query using the DataFrame's schema
    create_table_query = generate_create_table_query(df_pandas, table_name)
    client.command(create_table_query)

    # Insert the data into ClickHouse
    client.insert_df(table_name, df_pandas)


def get_latest_block_number(table_name: str, block_column: str, client) -> int:
    try:
        result = client.query(f"SELECT MAX({block_column}) FROM {table_name}")

        # Log the result to check what is actually returned
        logging.info(
            f"Query result for MAX({block_column}) in '{table_name}': {result}"
        )

        # Get max_block and handle None explicitly
        max_block = result.result_rows[0][0] if result.result_rows else None

        if max_block is None or max_block == "":
            logging.info(
                f"No existing blocks found in '{table_name}' or result is None. Starting from block 0."
            )
            return 0

        return int(max_block or 0)

    except clickhouse_connect.driver.exceptions.DatabaseError as e:
        error_msg = str(e)
        if "Code: 60" in error_msg or "UNKNOWN_TABLE" in error_msg:
            logging.info(f"Table '{table_name}' does not exist. Starting from block 0.")
            return 0
        else:
            raise e


async def fetch_l1_txs(l1_tx_list: Union[str, List[str]]) -> Optional[pl.DataFrame]:
    if not l1_tx_list:
        logger.info("No L1 transaction hashes to query.")
        return None

    if isinstance(l1_tx_list, str):
        l1_tx_list = [l1_tx_list]

    manager = HyperManager(url="https://holesky.hypersync.xyz")
    dataframes = []

    def chunked(iterable, n):
        for i in range(0, len(iterable), n):
            yield iterable[i : i + n]

    for chunk in chunked(l1_tx_list, 1000):
        try:
            l1_txs_chunk = await manager.search_txs(txs=chunk)
            if l1_txs_chunk is not None and not l1_txs_chunk.is_empty():
                dataframes.append(l1_txs_chunk)
        except Exception as e:
            logger.error(
                f"Unexpected error while fetching L1 transactions for chunk: {e}"
            )
            continue

    if not dataframes:
        logger.info("No L1 transactions found.")
        return None

    return pl.concat(dataframes)


async def get_events():
    manager = HyperManager(url="https://mev-commit.hypersync.xyz")
    logging.info("Connecting to ClickHouse database...")

    client = get_clickhouse_client()
    tables: List[Dict[str, Any]] = prepare_table_configs(event_names)

    latest_blocks = {}
    for table in tables:
        latest_block = get_latest_block_number(
            table["table_name"], table["block_column"], client
        )
        latest_blocks[table["table_name"]] = latest_block
    logging.info(
        "Latest blocks - " + "; ".join(f"{k}: {v}" for k, v in latest_blocks.items())
    )

    for table in tables:
        from_block = latest_blocks.get(table["table_name"], 0) + 1
        logger.info(f"Querying {table['table_name']} from block {from_block}...")
        try:
            df = await manager.execute_event_query(
                table["event_config"],
                tx_data=True,
                from_block=from_block,
            )
            record_count = len(df)
            if record_count > 0:
                write_to_clickhouse(df, table["table_name"], client)
                logger.info(
                    f"{table['table_name']}: {record_count} new records written."
                )
            else:
                logger.info(f"{table['table_name']}: 0 new records.")

            if table.get("special_handling") == "commit_stores" and not df.is_empty():
                l1_txs_list = (
                    df.with_columns((pl.lit("0x") + pl.col("txnHash")).alias("txnHash"))
                    .select("txnHash")
                    .unique()["txnHash"]
                    .to_list()
                )
                l1_txs_df = await fetch_l1_txs(l1_txs_list)
                if l1_txs_df is not None and not l1_txs_df.is_empty():
                    write_to_clickhouse(l1_txs_df, "l1_transactions", client)
                    logger.info(
                        f"l1_transactions: {len(l1_txs_df)} new records written."
                    )
                else:
                    logger.info("l1_transactions: 0 new records.")
        except ValueError as e:
            logger.error(f"Error querying {table['table_name']}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error querying {table['table_name']}: {e}")


if __name__ == "__main__":

    async def main_loop():
        while True:
            try:
                await get_events()
            except Exception as e:
                logger.error(f"Error in get_events: {e}")
            await asyncio.sleep(30)

    asyncio.run(main_loop())
