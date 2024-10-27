from datetime import datetime, timedelta
import polars as pl
from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Any, Optional, Union
from fastapi.middleware.cors import CORSMiddleware
from api.models import PreconfsResponse, AggregationResult, TableSchemaItem

from api.database import (
    get_commitments,
    get_db_connection,
    load_commitments_df,
    get_table_schema,
)

app = FastAPI(title="DuckDB Table Row Counts API")

# Enable CORS for the frontend to access the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can specify the frontend's origin for better security
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/tables", response_model=List[str])
def list_tables():
    """
    List all available tables in the DuckDB database.

    Returns:
        List[str]: A list of table names available in the DuckDB.

    Raises:
        HTTPException: If there is an error querying the database, returns a 500 status code.
    """
    try:
        with get_db_connection() as conn:
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
        return table_names
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/preconfs", response_model=PreconfsResponse)
def get_preconfs(
    page: int = Query(1, ge=1, description="Page number for pagination (default: 1)."),
    limit: int = Query(
        50, ge=1, le=100, description="Limit of items per page (default: 50)."
    ),
    hash: Optional[str] = Query(None, description="Filter by bid mev-commit hash."),
    block_number_l1: Optional[int] = Query(
        None, description="Filter by exact Layer 1 block number."
    ),
):
    """
    Get preconfs data with optional filters, paginated.

    Args:
        page (int): Page number for pagination (default: 1).
        limit (int): Number of rows per page (default: 50, maximum: 100).
        bidder (Optional[str]): Optional filter for bidder address.
        block_number_min (Optional[int]): Optional filter for minimum block number.
        block_number_max (Optional[int]): Optional filter for maximum block number.
        hash (Optional[str]): Optional filter for hash.
        block_number_l1 (Optional[int]): Optional filter for exact Layer 1 block number.

    Returns:
        dict: Paginated results with 'page', 'limit', 'total' rows, and the filtered data.

    Raises:
        HTTPException: If there is an error retrieving data, returns a 500 status code.
    """
    try:
        commitments_df = get_commitments(
            hash=hash,
            block_number_l1=block_number_l1,
        )

        total_rows = commitments_df.height
        offset = (page - 1) * limit

        paginated_df = commitments_df.sort("inc_block_number", descending=True).slice(
            offset, limit
        )

        result = paginated_df.to_dicts()

        return {"page": page, "limit": limit, "total": total_rows, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")


@app.get("/preconfs/aggregations", response_model=List[AggregationResult])
def aggregations(
    bidder: Optional[Union[List[str], str]] = Query(
        None, description="Optional filter by bidder(s)."
    ),
    provider: Optional[Union[List[str], str]] = Query(
        None, description="Optional filter by provider(s)."
    ),
    days: Optional[int] = Query(
        None, description="Filter by the number of past days (e.g., 1, 7, 30)."
    ),
):
    """
    Get group-by aggregations on preconfs data for bidders.

    Args:
        bidder (Optional[Union[List[str], str]]): Optional filter by one or more bidders.
        provider (Optional[Union[List[str], str]]): Optional filter by one or more providers.
        days (Optional[int]): Optional filter to aggregate data for the past 'n' days.

    Returns:
        List[AggregationResult]: Aggregated results with counts and bid-related calculations.

    Raises:
        HTTPException: If there is an error performing the aggregation, returns a 500 status code.
    """
    try:
        df = load_commitments_df()

        # Apply bidder filter if provided
        if bidder:
            if isinstance(bidder, str):
                df = df.filter(pl.col("bidder") == bidder)
            else:
                df = df.filter(pl.col("bidder").is_in(bidder))

        # Apply provider filter if provided
        if provider:
            if isinstance(provider, str):
                df = df.filter(pl.col("committer") == provider)
            else:
                df = df.filter(pl.col("committer").is_in(provider))

        # Apply date filter if `days` is provided
        if days:
            start_date = datetime.now() - timedelta(days=days)
            df = df.filter(pl.col("date") >= pl.lit(start_date))

        # Group by date and aggregate
        agg_df = (
            df.with_columns(pl.col("date").dt.round("1d"))
            .group_by("date")
            .agg(
                (pl.col("bid_eth").sum().alias("total_bid_eth")),
                (pl.col("decayed_bid_eth").sum().alias("total_decayed_bid_eth")),
                (pl.col("commitmentIndex").count().alias("preconf_count")),
                (pl.col("isSlash").sum().alias("slash_count")),
            )
            .sort(by="date")
        )

        # Convert to AggregationResult structure
        result = [
            AggregationResult(
                preconf_count=row["preconf_count"],
                average_bid=row["total_bid_eth"] / row["preconf_count"]
                if row["preconf_count"] > 0
                else 0,
                total_bid=row["total_bid_eth"],
                total_decayed_bid=row["total_decayed_bid_eth"],
                slash_count=row["slash_count"],
                group_by_value=row["date"].isoformat(),
            )
            for row in agg_df.to_dicts()
        ]

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}/schema", response_model=List[TableSchemaItem])
def get_table_schema_endpoint(table_name: str):
    """
    Retrieve the schema of a specified DuckDB table.

    Args:
        table_name (str): Name of the table to retrieve the schema from.

    Returns:
        List[Dict[str, str]]: A list of column names and their data types for the specified table.

    Raises:
        HTTPException: If there is an error retrieving the table schema, returns a 500 status code.
    """
    try:
        schema = get_table_schema(table_name)
        return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
