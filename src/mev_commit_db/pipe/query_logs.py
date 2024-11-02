import polars as pl
from hypermanager.events import EventConfig
from hypermanager.manager import HyperManager
from typing import Optional


async def fetch_event_for_config(
    manager: HyperManager, base_event_config: EventConfig, block_number: int
) -> Optional[pl.DataFrame]:
    """
    Fetch event logs for a single event configuration.

    Parameters:
    - base_event_config (EventConfig): The event configuration for which to fetch event logs.
    - block_number (int): The block number to start the query from

    Returns:
    - Optional[pl.DataFrame]: A Polars DataFrame containing the fetched event logs, or None if no events were found.
    """
    try:
        # Query events using the event configuration and return the result as a Polars DataFrame
        df: pl.DataFrame = await manager.execute_event_query(
            base_event_config, tx_data=True, from_block=block_number
        )

        if df.is_empty():
            print(f"No events found for {base_event_config.name}, continuing...")
            return None

        print(f"Events found for {base_event_config.name}:")
        print(df.shape)
        return df

    except Exception as e:
        print(f"Error querying {base_event_config.name}: {e}")
        return None  # Return None in case of an error


# async def get_events() -> Dict[str, pl.DataFrame]:
#     """
#     Fetch and aggregate event logs for all configurations in mev_commit_config.

#     Returns:
#     - Dict[str, pl.DataFrame]: A dictionary with event names as keys and non-empty Polars DataFrames as values.
#     """
#     # Create a list of tasks for each event configuration, including the event name for reference
#     tasks: List[Tuple[asyncio.Task, str]] = [
#         (fetch_event_for_config(config), config.name)
#         for config in mev_commit_config.values()
#     ]

#     # Gather results for each task, and only retain non-None results paired with their event names
#     results: List[Optional[pl.DataFrame]] = await asyncio.gather(
#         *[task[0] for task in tasks]
#     )

#     # Build the dictionary with event names as keys and non-None DataFrames as values
#     return {task[1]: df for df, task in zip(results, tasks) if df is not None}
