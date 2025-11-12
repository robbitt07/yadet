from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, Optional

# Valid data types for order_by_columns
VALID_DTYPES = {"str", "int", "date", "datetime"}


class TableConfig(BaseModel):
    """Configuration for a single table extraction."""
    
    ordinal: int = Field(..., ge=0, description="Order in which tables are processed")
    table_name: str = Field(..., min_length=1, description="Source table name")
    delta: bool = Field(..., description="Whether to use incremental/delta extraction")
    order_by_columns: Dict[str, str] = Field(
        ..., 
        min_length=1,
        description="Dictionary mapping column names to their data types for ordering"
    )
    table_alias: Optional[str] = Field(None, description="Optional alias for the table")
    output_table_name: Optional[str] = Field(None, description="Optional output table name")
    columns: str = Field("*", description="Columns to select (default: *)")
    filter_clause: Optional[str] = Field(None, description="Optional WHERE clause filter")
    join_clause: Optional[str] = Field(None, description="Optional JOIN clause")
    flat: bool = Field(True, description="Whether to flatten the result")
    batch_size: int = Field(5000, gt=0, le=100000, description="Number of records per batch")
    min_wait: Optional[int] = Field(
        None, ge=0, description="Minimum wait time in minutes between runs"
    )
    active: bool = Field(True, description="Whether this table config is active")
    desc: Optional[str] = Field(None, description="Optional description")
    
    @field_validator("order_by_columns")
    @classmethod
    def validate_order_by_columns(cls, v: Dict[str, str]) -> Dict[str, str]:
        """Validate that all data types in order_by_columns are valid."""
        if not v:
            raise ValueError("order_by_columns cannot be empty")
        
        for column, dtype in v.items():
            if not column or not column.strip():
                raise ValueError(f"Column name cannot be empty in order_by_columns")
            if dtype not in VALID_DTYPES:
                raise ValueError(
                    f"Invalid dtype `{dtype}` for column `{column}`. "
                    f"Valid dtypes are: {', '.join(sorted(VALID_DTYPES))}"
                )
        return v
    
    @field_validator("table_name", "columns")
    @classmethod
    def validate_not_empty_string(cls, v: str) -> str:
        """Validate that string fields are not empty."""
        if not v or not v.strip():
            raise ValueError("Field cannot be empty")
        return v.strip()
    
    @field_validator("table_alias", "output_table_name", "filter_clause", "join_clause", "desc")
    @classmethod
    def validate_optional_strings(cls, v: Optional[str]) -> Optional[str]:
        """Validate and strip optional string fields."""
        if v is not None:
            v = v.strip() if isinstance(v, str) else v
            if v == "":
                return None
        return v
    
    @model_validator(mode="after")
    def validate_delta_requires_order_by(self) -> "TableConfig":
        """Validate that delta extraction requires order_by_columns."""
        if self.delta and not self.order_by_columns:
            raise ValueError("delta=True requires at least one order_by_columns entry")
        return self
    
    @property
    def get_table_alias(self) -> str:
        """Get the table alias, or table_name if alias is not set."""
        return self.table_alias or self.table_name
    
    @property
    def get_output_table_name(self) -> str:
        """Get the output table name, or table alias if not set."""
        return self.output_table_name or self.get_table_alias
    
    def model_dump(self, **kwargs) -> Dict:
        """Override model_dump to use 'data' for backward compatibility."""
        return super().model_dump(**kwargs)
    
    @property
    def data(self) -> Dict:
        """Backward compatibility property for model_dump."""
        return self.model_dump(exclude_none=False)
    
    def __str__(self) -> str:
        """String representation of the table config."""
        return f"{self.get_table_alias}: {self.ordinal}"
    
    class Config:
        """Pydantic v2 configuration."""
        json_schema_extra = {
            "example": {
                "ordinal": 100,
                "table_name": "SalesLT.Customer",
                "delta": False,
                "order_by_columns": {
                    "ModifiedDate": "datetime"
                },
                "table_alias": "cust",
                "output_table_name": None,
                "columns": "*",
                "filter_clause": None,
                "join_clause": None,
                "flat": True,
                "batch_size": 5000,
                "min_wait": 15,
                "active": True,
                "desc": None
            }
        }
