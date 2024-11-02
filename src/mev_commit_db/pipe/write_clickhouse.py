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
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}) ENGINE = MergeTree ORDER BY tuple()"

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
