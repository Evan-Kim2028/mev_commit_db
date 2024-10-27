# database.py

import os
import duckdb
import polars as pl
import logging
from typing import Dict, List, Optional
from mev_commit_db.db_lock import (
    acquire_lock,
    release_lock,
)  # Import the locking functions
from fastapi import HTTPException  # Only import HTTPException for error handling
from api.utils import byte_to_string

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the database filename from the environment variable
DB_FILENAME = os.getenv("DATABASE_URL", "/app/db_data/mev_commit.duckdb")


def get_db_connection():
    """Establishes and returns a DuckDB connection."""
    try:
        return duckdb.connect(DB_FILENAME, read_only=True)
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")


def load_commitments_df() -> pl.DataFrame:
    """
    Loads data from encrypted_stores, commits_processed, and commit_stores and joins
    them together to create a unified view of preconfirmation data.
    """
    # Acquire lock before accessing DuckDB
    lockfile = acquire_lock()
    try:
        conn = get_db_connection()

        # Read the tables
        encrypted_stores_df = conn.execute(
            "SELECT * FROM unopenedcommitmentstored"
        ).pl()
        commit_stores_df = conn.execute("SELECT * FROM openedcommitmentstored").pl()
        commits_processed_df = conn.execute("SELECT * FROM commitmentprocessed").pl()
        l1_txs = conn.execute("SELECT * FROM l1_transactions").pl()

        conn.close()

        # Perform joins
        commitments_df = (
            encrypted_stores_df.select(
                "commitmentIndex", "committer", "commitmentDigest"
            )
            .join(
                commit_stores_df,
                on="commitmentIndex",
                how="inner",
                suffix="_opened_commit",
            )
            .with_columns((pl.lit("0x") + pl.col("txnHash")).alias("txnHash"))
            .join(
                commits_processed_df.select("commitmentIndex", "isSlash"),
                on="commitmentIndex",
                how="inner",
            )
            .join(
                l1_txs,
                left_on="txnHash",
                right_on="hash",
                suffix="_l1",
            )
            .rename(
                {"blockNumber": "inc_block_number"}  # desired block number for preconf
            )
            .with_columns(
                (pl.col("bid") / 10**18).alias("bid_eth"),
                pl.from_epoch("timestamp", time_unit="ms").alias("date"),
                pl.col("extra_data_l1")
                .map_elements(byte_to_string, return_dtype=str)
                .alias("builder_graffiti"),
            )
            # bid decay calculations
            # the formula to calculate the bid decay = (decayEndTimeStamp - decayStartTimeStamp) / (dispatchTimestamp - decayEndTimeStamp). If it's a negative number, then bid would have decayed to 0
            .with_columns(
                # need to change type from uint to int to account for negative numbers
                pl.col("decayStartTimeStamp").cast(pl.Int64),
                pl.col("decayEndTimeStamp").cast(pl.Int64),
                pl.col("dispatchTimestamp").cast(pl.Int64),
            )
            .with_columns(
                (pl.col("decayEndTimeStamp") - pl.col("decayStartTimeStamp")).alias(
                    "decay_range"
                ),
                (pl.col("decayEndTimeStamp") - pl.col("dispatchTimestamp")).alias(
                    "dispatch_range"
                ),
            )
            .with_columns(
                (pl.col("dispatch_range") / pl.col("decay_range")).alias(
                    "decay_multiplier"
                )
            )
            .with_columns(
                pl.when(pl.col("decay_multiplier") < 0)
                .then(0)
                .otherwise(pl.col("decay_multiplier"))
            )
            # calculate decayed bid. The decay multiplier is the amount that the bid decays by.
            .with_columns(
                (pl.col("decay_multiplier") * pl.col("bid_eth")).alias(
                    "decayed_bid_eth"
                )
            )
        )

        # Select desired columns
        commitments_df = commitments_df.select(
            "commitmentIndex",
            "committer",
            "commitmentDigest",
            "bidder",
            "isSlash",
            "commitmentSignature",
            "bid",
            "inc_block_number",
            "bidHash",
            "decayStartTimeStamp",
            "decayEndTimeStamp",
            "txnHash",
            "revertingTxHashes",
            "bidSignature",
            "sharedSecretKey",
            "block_number",  # mev-commit block number
            # the l1 transaction data
            "block_number_l1",
            "extra_data_l1",
            "to_l1",
            "from_l1",
            "nonce_l1",
            "type_l1",
            "block_hash_l1",
            "timestamp_l1",
            "base_fee_per_gas_l1",
            "gas_used_block_l1",
            "parent_beacon_block_root",
            "max_priority_fee_per_gas_l1",
            "max_fee_per_gas_l1",
            "effective_gas_price_l1",
            "gas_used_l1",
            "date",
            "bid_eth",
            "decayed_bid_eth",
            "dispatch_range",
            "decay_multiplier",
            "builder_graffiti",
        )

        return commitments_df
    except Exception as e:
        logger.error(f"Error loading commitments data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        # Release the lock after operation is done
        release_lock(lockfile)


def get_commitments(
    hash: Optional[str] = None,
    block_number_l1: Optional[int] = None,
) -> pl.DataFrame:
    """
    Retrieve preconf commitments with optional filtering by bidder, block numbers, and hash.

    Args:
        block_number_min (Optional[int]): Optional filter for minimum block number.
        block_number_max (Optional[int]): Optional filter for maximum block number.
        hash (Optional[str]): Optional filter for hash.
        block_number_l1 (Optional[int]): Optional filter for exact Layer 1 block number.

    Returns:
        pl.DataFrame: Filtered DataFrame containing commitments.

    Raises:
        HTTPException: If there is an error retrieving data, returns a 500 status code.
    """
    try:
        df = load_commitments_df()

        # Apply hash filter
        if hash:
            df = df.filter(pl.col("bidHash") == hash)

        # Apply exact Layer 1 block number filter
        if block_number_l1 is not None:
            df = df.filter(pl.col("block_number_l1") == block_number_l1)

        return df
    except Exception as e:
        logger.error(f"Error retrieving commitments: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


def get_table_schema(table_name: str) -> List[Dict[str, str]]:
    """
    Retrieve the schema of the specified table.

    Args:
        table_name (str): Name of the table whose schema is to be retrieved.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing column names and their data types.
    """
    try:
        # Acquire lock before accessing DuckDB
        lockfile = acquire_lock()
        conn = get_db_connection()

        # Use DuckDB's information_schema to get column details
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """

        result = conn.execute(query).fetchall()
        conn.close()

        if not result:
            logger.error(f"Table '{table_name}' does not exist.")
            raise HTTPException(
                status_code=404, detail=f"Table '{table_name}' not found."
            )

        # Format the schema as a list of dictionaries
        schema = [{"column_name": row[0], "data_type": row[1]} for row in result]
        return schema

    except HTTPException as he:
        raise he  # Re-raise HTTP exceptions

    except Exception as e:
        logger.error(f"Error retrieving schema for table '{table_name}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

    finally:
        # Release the lock after operation is done
        release_lock(lockfile)
