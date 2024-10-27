import asyncio
import os
import polars as pl
import logging
from typing import Any, Dict, List, Union, Optional
from contextlib import contextmanager
from hypermanager.events import EventConfig
from hypermanager.manager import HyperManager
from hypermanager.protocols.mev_commit import mev_commit_config
from data_processing import get_latest_block_number, write_to_duckdb
from mev_commit_db.db_lock import acquire_lock, release_lock

db_dir = "data"

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


@contextmanager
def db_write_context(table_name: str, db_filename: str):
    """
    Context manager to handle locking while writing to DuckDB.
    """
    lockfile = acquire_lock()
    try:
        yield
    finally:
        release_lock(lockfile)


async def fetch_l1_txs(l1_tx_list: Union[str, list[str]]) -> Optional[pl.DataFrame]:
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
    os.makedirs(db_dir, exist_ok=True)
    logging.info(f"Ensured directory exists: {db_dir}")

    db_filename = os.path.join(db_dir, "mev_commit.duckdb")
    tables: List[Dict[str, Any]] = prepare_table_configs(event_names)

    latest_blocks = {}
    for table in tables:
        latest_block = get_latest_block_number(
            table["table_name"], table["block_column"], db_filename
        )
        latest_blocks[table["table_name"]] = latest_block
    logging.info(
        "Latest blocks - " + "; ".join(f"{k}: {v}" for k, v in latest_blocks.items())
    )

    for table in tables:
        from_block = latest_blocks.get(table["table_name"], 0) + 1
        logger.info(f"Querying {table['table_name']} from block {from_block}...")
        try:
            df: pl.DataFrame = await manager.execute_event_query(
                table["event_config"],
                tx_data=True,
                from_block=from_block,
            )
            record_count = len(df)
            if record_count > 0:
                with db_write_context(table["table_name"], db_filename):
                    write_to_duckdb(df, table["table_name"], db_filename)
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
                    with db_write_context("l1_transactions", db_filename):
                        write_to_duckdb(l1_txs_df, "l1_transactions", db_filename)
                        logger.info(
                            f"l1_transactions: {len(l1_txs_df)} new records written."
                        )
                else:
                    logger.info("l1_transactions: 0 new records.")
        except ValueError as e:
            logger.error(f"Error querying {table['table_name']}: {e}")


if __name__ == "__main__":

    async def main_loop():
        while True:
            try:
                await get_events()
            except Exception as e:
                logger.error(f"Error in get_events: {e}")
            await asyncio.sleep(30)

    asyncio.run(main_loop())
