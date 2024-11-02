import asyncio
import clickhouse_connect
from clickhouse_connect.driver import Client
from src.mev_commit_db.pipe.query_logs import get_events
from src.mev_commit_db.pipe.query_clickhouse import write_to_clickhouse


async def main():
    """
    Main function to fetch event logs in a loop, convert them, and write them to ClickHouse every 30 seconds.
    """
    # Connect to ClickHouse
    client: Client = clickhouse_connect.get_client(
        database="mev_commit_testnet",
        host="localhost",
        port=8123,
        username="default",
        password="",
    )

    try:
        while True:
            print("Fetching new events...")

            # Fetch event data as Polars DataFrames from the get_events() function
            events_data = await get_events()

            # Iterate over each event and write to ClickHouse
            for event_name, df in events_data.items():
                print(f"Processing event: {event_name}")

                # Write the data to ClickHouse using write_to_clickhouse
                write_to_clickhouse(client, event_name, df)

            # Wait for 30 seconds before fetching new data
            await asyncio.sleep(30)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the ClickHouse connection after all writes are done
        client.close_connections()


# Run the main async function
if __name__ == "__main__":
    asyncio.run(main())
