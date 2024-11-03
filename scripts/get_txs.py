import asyncio
import clickhouse_connect
import logging
import os
import polars as pl

from clickhouse_connect.driver import Client
from mev_commit_db.pipe.hypersync import fetch_txs
from mev_commit_db.pipe.write_clickhouse import write_to_clickhouse
from mev_commit_db.pipe.clickhouse import get_max_column_val

from dotenv import load_dotenv

load_dotenv()
# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


async def main():
    """
    Main function to fetch event logs in a loop, convert them, and write them to ClickHouse every 30 seconds.
    """
    client: Client = clickhouse_connect.get_client(
        database=os.getenv("CLICKHOUSE_DATABASE"),
        host=os.getenv("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT")),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
    )
    try:
        while True:
            logger.info("Fetching preconfirmed txs")
            table_name = "L1Transactions"
            # 1. First get the max block number for the table L1Transactions table. If it doesn't exist, then the default block number is 0.
            max_block_number: int = get_max_column_val(
                client, table_name, column_name="block_number"
            )

            # 2. Get the transaction list to lookup
            query = f"""
                SELECT *
                FROM mev_commit_testnet.OpenedCommitmentStored
                WHERE blockNumber > {max_block_number}
            """
            query_result = client.query_df(query)

            if query_result.empty:
                logger.info("No new records found in OpenedCommitmentStored table.")
                l1_tx_list = []
            else:
                # Log the columns for debugging
                logger.debug(f"Columns in query_result: {list(query_result.columns)}")
                # Check if 'txnHash' is a column in query_result
                if "txnHash" in query_result.columns:
                    l1_tx_list: list[str] = query_result["txnHash"].tolist()
                else:
                    logger.error(
                        f"Column 'txnHash' not found in query_result columns: {list(query_result.columns)}"
                    )
                    l1_tx_list = []
                    continue

            # Proceed only if l1_tx_list is not empty
            if l1_tx_list:
                l1_txs_df: pl.DataFrame = await fetch_txs(
                    l1_tx_list, "https://holesky.hypersync.xyz"
                )
                # 4. Write to clickhouse
                if l1_txs_df is not None and not l1_txs_df.is_empty():
                    logger.info(
                        f"Fetched {len(l1_txs_df)} rows for {table_name}. Writing to ClickHouse."
                    )
                    write_to_clickhouse(client, table_name, l1_txs_df)
                    logger.info(f"Successfully wrote data to table {table_name}")
                else:
                    logger.info("No data fetched from fetch_txs.")
            else:
                logger.info("No transaction hashes to process.")

            logger.info("Sleeping for 30 seconds before next fetch cycle.")
            await asyncio.sleep(30)

    except Exception as e:
        logger.error(f"An error occurred in main loop: {e}", exc_info=True)

    finally:
        client.close_connections()
        logger.info("ClickHouse connection closed.")


if __name__ == "__main__":
    logger.info("Starting the async main loop.")
    asyncio.run(main())
    logger.info("Finished executing the async main loop.")
