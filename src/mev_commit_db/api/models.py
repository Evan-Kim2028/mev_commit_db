from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Optional, Union


class PreconfDataItem(BaseModel):
    commitmentIndex: str = Field(
        ...,
        description="Hexadecimal hash representing the commitment index.",
        example="0x55286507e3699888db944de5fbdcb4109f73e3e7c1f68065fb9d9a6c955553db",
    )
    committer: str = Field(
        ...,
        description="Address of the committer.",
        example="0x2445e5e28890de3e93f39fca817639c470f4d3b9",
    )
    commitmentDigest: str = Field(
        ...,
        description="Digest of the commitment.",
        example="0x80443d1c483a0d863e5ed33f91385cd0e8f8a8c173315043508e20d3a234ec09",
    )
    bidder: str = Field(
        ...,
        description="Address of the bidder.",
        example="0xe51ef1836dbef052bffd2eb3fe1314365d23129d",
    )
    isSlash: bool = Field(
        ..., description="Indicates if the commitment is slashed.", example=False
    )
    commitmentSignature: str = Field(
        ...,
        description="Signature of the commitment.",
        example="0xf4763d17cb9b92b9e6f424b32c0fc5901dcd232cc91d7aa793eabd0eea4e2632488974019e66c067b60bd7157de199d7b0b73451854503bcb29a4981216d1c721b",
    )
    bid: int = Field(
        ..., description="Amount bid by the bidder.", example=89287886889814930
    )
    inc_block_number: int = Field(
        ..., description="Block number in the incremental layer.", example=2581126
    )
    bidHash: str = Field(
        ...,
        description="Hash of the bid.",
        example="0x7211ac9299337d69e11bb9e08de5bb4ee3fc65461228554039e4fc0a2d4e1d6a",
    )
    decayStartTimeStamp: int = Field(
        ..., description="Timestamp when decay starts.", example=1729551277865
    )
    decayEndTimeStamp: int = Field(
        ..., description="Timestamp when decay ends.", example=1729551313865
    )
    txnHash: str = Field(
        ...,
        description="Transaction hash associated with the commitment.",
        example="0x16f670c74bd86cddb149e66b9c2aabf8b9a5bf801096c6dd2ab1843c27d004f5",
    )
    revertingTxHashes: Optional[str] = Field(
        None, description="Hashes of transactions that are reverting.", example=""
    )
    bidSignature: str = Field(
        ...,
        description="Signature of the bid.",
        example="0x10d63e069c5de970a0895ee1c4bc7f3070ca8b794691e48f3326a6069f72226f7e7b197d92fc79ab0fc6f056342481849fa76e21092135bf82bc9e59b0f5bbdf1c",
    )
    sharedSecretKey: str = Field(
        ...,
        description="Shared secret key for the commitment.",
        example="0xe02cb5b79ae395f3047dab2deaf78a68d0794ef9faf7533c83afb2dc39caa881",
    )
    block_number: int = Field(
        ..., description="Block number where the commitment was made.", example=31996874
    )
    block_number_l1: int = Field(
        ..., description="Exact Layer 1 block number.", example=2581126
    )
    extra_data_l1: Optional[str] = Field(
        None,
        description="Additional data related to Layer 1.",
        example="0x707265636f6e662e6275696c646572",
    )
    to_l1: str = Field(
        ...,
        description="Recipient address in Layer 1.",
        example="0xe51ef1836dbef052bffd2eb3fe1314365d23129d",
    )
    from_l1: str = Field(
        ...,
        description="Sender address in Layer 1.",
        example="0xe51ef1836dbef052bffd2eb3fe1314365d23129d",
    )
    nonce_l1: int = Field(
        ..., description="Nonce used in Layer 1 transaction.", example=19926
    )
    type_l1: int = Field(
        ..., description="Type identifier for Layer 1 transaction.", example=2
    )
    block_hash_l1: str = Field(
        ...,
        description="Hash of the Layer 1 block.",
        example="0x783611d5e8bcf25b4d3f7dbf2b9a9b7a6c75cd8f4002b2eb6f46a543e8368c6d",
    )
    timestamp_l1: int = Field(
        ..., description="Timestamp of the Layer 1 block.", example=1729551300
    )
    base_fee_per_gas_l1: int = Field(
        ..., description="Base fee per gas unit in Layer 1.", example=8
    )
    gas_used_block_l1: int = Field(
        ..., description="Gas used in the Layer 1 block.", example=1015603
    )
    parent_beacon_block_root: str = Field(
        ...,
        description="Parent beacon block root in Layer 1.",
        example="0x7a703c7c05475d3e77f7562ec3f8e386c93ee7434673291a1321cbd492401a10",
    )
    max_priority_fee_per_gas_l1: int = Field(
        ..., description="Maximum priority fee per gas unit in Layer 1.", example=0
    )
    max_fee_per_gas_l1: int = Field(
        ..., description="Maximum fee per gas unit in Layer 1.", example=32
    )
    effective_gas_price_l1: int = Field(
        ..., description="Effective gas price in Layer 1.", example=8
    )
    gas_used_l1: int = Field(
        ..., description="Total gas used in Layer 1.", example=21000
    )

    date: datetime = Field(
        ...,
        description="Date and time of the commitment.",
        example="2024-04-01T12:34:56Z",
    )
    bid_eth: float = Field(..., description="Bid amount in ETH.", example=89.28788689)
    decayed_bid_eth: float = Field(
        ..., description="Decayed bid amount in ETH.", example=50.0
    )
    dispatch_range: int = Field(..., description="Dispatch range value.", example=12345)
    decay_multiplier: float = (
        Field(..., description="Decay multiplier used for bid decay.", example=0.5),
    )
    builder_graffiti: str = Field(
        ..., description="Builder graffiti.", example="preconf.builder"
    )


class PreconfsResponse(BaseModel):
    page: int = Field(..., description="Current page number.", example=1)
    limit: int = Field(..., description="Number of items per page.", example=50)
    total: int = Field(..., description="Total number of items available.", example=1)
    data: List[PreconfDataItem] = Field(
        ...,
        description="List of preconf data items.",
        example=[
            {
                "commitmentIndex": "0x55286507e3699888db944de5fbdcb4109f73e3e7c1f68065fb9d9a6c955553db",
                "committer": "0x2445e5e28890de3e93f39fca817639c470f4d3b9",
                "commitmentDigest": "0x80443d1c483a0d863e5ed33f91385cd0e8f8a8c173315043508e20d3a234ec09",
                "bidder": "0xe51ef1836dbef052bffd2eb3fe1314365d23129d",
                "isSlash": False,
                "commitmentSignature": "0xf4763d17cb9b92b9e6f424b32c0fc5901dcd232cc91d7aa793eabd0eea4e2632488974019e66c067b60bd7157de199d7b0b73451854503bcb29a4981216d1c721b",
                "bid": 89287886889814930,
                "inc_block_number": 2581126,
                "bidHash": "0x7211ac9299337d69e11bb9e08de5bb4ee3fc65461228554039e4fc0a2d4e1d6a",
                "decayStartTimeStamp": 1729551277865,
                "decayEndTimeStamp": 1729551313865,
                "txnHash": "0x16f670c74bd86cddb149e66b9c2aabf8b9a5bf801096c6dd2ab1843c27d004f5",
                "revertingTxHashes": "",
                "bidSignature": "0x10d63e069c5de970a0895ee1c4bc7f3070ca8b794691e48f3326a6069f72226f7e7b197d92fc79ab0fc6f056342481849fa76e21092135bf82bc9e59b0f5bbdf1c",
                "sharedSecretKey": "0xe02cb5b79ae395f3047dab2deaf78a68d0794ef9faf7533c83afb2dc39caa881",
                "block_number": 31996874,
                "block_number_l1": 2581126,
                "extra_data_l1": "0x707265636f6e662e6275696c646572",
                "to_l1": "0xe51ef1836dbef052bffd2eb3fe1314365d23129d",
                "from_l1": "0xe51ef1836dbef052bffd2eb3fe1314365d23129d",
                "nonce_l1": 19926,
                "type_l1": 2,
                "block_hash_l1": "0x783611d5e8bcf25b4d3f7dbf2b9a9b7a6c75cd8f4002b2eb6f46a543e8368c6d",
                "timestamp_l1": 1729551300,
                "base_fee_per_gas_l1": 8,
                "gas_used_block_l1": 1015603,
                "parent_beacon_block_root": "0x7a703c7c05475d3e77f7562ec3f8e386c93ee7434673291a1321cbd492401a10",
                "max_priority_fee_per_gas_l1": 0,
                "max_fee_per_gas_l1": 32,
                "effective_gas_price_l1": 8,
                "gas_used_l1": 21000,
                "date": "2024-10-21T19:25:44.211000",
                "bid_eth": 0.05017327452651951,
                "decayed_bid_eth": 0.05000045546981705,
                "dispatch_range": 35876,
                "decay_multiplier": 0.9965555555555555,
                "builder_graffiti": "preconf.builder",
            }
        ],
    )


class AggregationResult(BaseModel):
    preconf_count: int = Field(
        ..., description="Total number of preconf commitments.", example=89
    )
    average_bid: float = Field(..., description="Average bid amount.", example=0.496947)
    total_bid: float = Field(..., description="Total bid amount.", example=0.7093)
    total_decayed_bid: float = Field(
        ..., description="Total decayed bid amount.", example=0.353052
    )
    slash_count: int = Field(
        ..., description="Total number of slash commitments.", example=113
    )
    group_by_value: Union[str, int] = Field(
        ...,
        description="Value by which the results are grouped (e.g., date, bidder).",
        example="2024-08-13",
    )

    class Config:
        schema_extra = {
            "example": {
                "preconf_count": 89,
                "average_bid": 0.496947,
                "total_bid": 0.7093,
                "total_decayed_bid": 0.353052,
                "slash_count": 113,
                "group_by_value": "2024-08-13",
            }
        }


class TableSchemaItem(BaseModel):
    column_name: str = Field(
        ..., description="Name of the table column.", example="commitmentIndex"
    )
    data_type: str = Field(..., description="Data type of the column.", example="str")

    class Config:
        schema_extra = {
            "example": {"column_name": "commitmentIndex", "data_type": "str"}
        }
