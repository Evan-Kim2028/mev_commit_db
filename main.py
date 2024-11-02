import asyncio
import clickhouse_connect
from clickhouse_connect.driver import Client
from src.mev_commit_db.pipe.query_logs import fetch_event_for_config
from mev_commit_db.pipe.write_clickhouse import write_to_clickhouse
from mev_commit_db.pipe.query_clickhouse import get_max_column_val
from hypermanager.protocols.mev_commit import mev_commit_config
from hypermanager.manager import HyperManager
import logging

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


async def main():
    """
    Main function to fetch event logs in a loop, convert them, and write them to ClickHouse every 30 seconds.
    """
    logger.info("Starting main function to fetch and process event logs.")

    # Connect to ClickHouse
    client: Client = clickhouse_connect.get_client(
        database="mev_commit_testnet",
        host="localhost",
        port=8123,
        username="default",
        password="",
    )
    manager = HyperManager("https://mev-commit.hypersync.xyz")

    try:
        while True:
            logger.info("Fetching new events...")

            # Use asynchronous tasks to query each event configuration in parallel
            tasks = [
                process_config(client, manager, config)
                for config in mev_commit_config.values()
            ]
            await asyncio.gather(*tasks)

            logger.info("Sleeping for 30 seconds before next fetch cycle.")
            # Wait for 30 seconds before fetching new data
            await asyncio.sleep(30)

    except Exception as e:
        logger.error(f"An error occurred in main loop: {e}", exc_info=True)

    finally:
        # Close the ClickHouse connection after all writes are done
        client.close_connections()
        logger.info("ClickHouse connection closed.")


async def process_config(client, manager, config):
    """
    Asynchronously process a single event configuration.
    """
    logger.info(f"Starting to process configuration: {config.name}")

    try:
        # Get the maximum block number for the corresponding table
        max_block_number: int = get_max_column_val(client, config.name)
        logger.debug(f"Max block number for {config.name} is {max_block_number}")

        # Fetch events for the specific configuration and block range
        df = await fetch_event_for_config(
            manager=manager,
            base_event_config=config,
            block_number=max_block_number,
        )

        if df is not None:
            logger.info(
                f"Fetched {len(df)} rows for {config.name}. Writing to ClickHouse."
            )
            write_to_clickhouse(client, config.name, df)
            logger.info(f"Successfully wrote data to table {config.name}")
        else:
            logger.warning(f"No data to write for {config.name}")

    except Exception as e:
        logger.error(f"Error processing {config.name}: {e}", exc_info=True)


# Run the main async function
if __name__ == "__main__":
    logger.info("Starting the async main loop.")
    asyncio.run(main())
    logger.info("Finished executing the async main loop.")
