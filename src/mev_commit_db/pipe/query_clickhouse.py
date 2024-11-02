import logging
from clickhouse_connect.driver import Client
from clickhouse_driver.errors import ServerException, ErrorCodes

# Configure logging for this module
logger = logging.getLogger(__name__)


def get_max_column_val(
    client: Client, table_name: str, column_name: str = "block_number"
) -> int:
    """
    Query the maximum value of a specified column (default is 'block_number') from a given table.

    Args:
        client (Client): The ClickHouse client to use for querying.
        table_name (str): The name of the table to query.
        column_name (str): The name of the column to get the maximum value from.

    Returns:
        int: The maximum value of the specified column, or 0 if the table/column doesn't exist or an error occurs.
    """
    query = f"SELECT MAX({column_name}) AS max_block_number FROM {table_name}"
    try:
        result = client.query(query)
        max_block_number = result.result_rows[0][0]  # Extract the max block number
        logger.info(
            f"Max block number in table '{table_name}' is {max_block_number + 1}"
        )
        return max_block_number + 1
    except ServerException as e:
        # Check for error code 60 ("Table doesn't exist")
        if e.code == ErrorCodes.UNKNOWN_TABLE:
            # logger.warning(
            #     f"Table '{table_name}' does not exist. Returning 0 as max block number."
            # )
            return 0
        else:
            # logger.error(f"Failed to query max block number from {table_name}: {e}")
            return 0
    except Exception as e:
        # logger.error(
        #     f"Failed to query max block number from {table_name}: {e}. Returning 0"
        # )
        return 0
