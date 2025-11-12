from yadet.config.table import TableConfig
from yadet.engine.source.base import SourceMssqlEngine
from yadet.engine.source.mssql import MsSqlSourceEngine
from yadet.engine.target.base import TargetFileSystemEngine
from yadet.engine.target.file_system import FileSystemTargetEngine
from yadet.helpers.json import meta_handler
from yadet.errors import ConfigException

from pydantic import BaseModel, Field, field_validator, model_validator, PrivateAttr
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Union, Any


class ProjectConfig(BaseModel):
    """Configuration for a data extraction project."""
    
    name: str = Field(..., min_length=1, description="Project name")
    source_engine: str = Field(
        SourceMssqlEngine, 
        description="Source engine type identifier"
    )
    source_engine_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters for the source engine"
    )
    target_engine: str = Field(
        TargetFileSystemEngine,
        description="Target engine type identifier"
    )
    target_engine_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters for the target engine"
    )
    config_dir: Union[str, os.PathLike] = Field(
        "config",
        description="Directory where configuration files are stored"
    )
    
    # Internal state (not part of model fields)
    _tables_config: List[TableConfig] = PrivateAttr(default_factory=list)
    _project_table_index: Dict[str, Dict] = PrivateAttr(default_factory=dict)
    
    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate project name is not empty."""
        if not v or not v.strip():
            raise ValueError("Project name cannot be empty")
        return v.strip()
    
    @field_validator("source_engine", "target_engine")
    @classmethod
    def validate_engine(cls, v: str) -> str:
        """Validate engine identifiers."""
        if not v or not v.strip():
            raise ValueError("Engine identifier cannot be empty")
        return v.strip()
    
    @model_validator(mode="after")
    def validate_target_params(self) -> "ProjectConfig":
        """Validate that target_engine_params contains required base_directory."""
        if self.target_engine == TargetFileSystemEngine:
            if "base_directory" not in self.target_engine_params:
                raise ValueError(
                    "target_engine_params must contain 'base_directory' for file_system engine"
                )
        return self
    
    @property
    def project_directory(self) -> os.PathLike:
        """Get the project directory path."""
        return os.path.join(self.config_dir, self.name)
    
    @property
    def table_config_filename(self) -> os.PathLike:
        """Get the table configuration file path."""
        return os.path.join(self.project_directory, "project_config.json")
    
    @property
    def table_index_filename(self) -> os.PathLike:
        """Get the table index file path."""
        return os.path.join(
            self.target_engine_params.get("base_directory", ""),
            "project_table_index.json"
        )
    
    @property
    def tables_config(self) -> List[TableConfig]:
        """Get sorted list of table configurations."""
        return sorted(self._tables_config, key=lambda x: x.ordinal)
    
    @property
    def project_table_index(self) -> Dict[str, Dict]:
        """Get the project table index."""
        return self._project_table_index
    
    @property
    def base_config(self) -> Dict:
        """Get base configuration dictionary."""
        return {
            "name": self.name,
            "source_engine": self.source_engine,
            "target_engine": self.target_engine
        }
    
    def model_post_init(self, __context: Any) -> None:
        """Post-initialization hook to load configuration from files."""
        self.open()
    
    def open(self) -> None:
        """Open and load project configuration from files."""
        # Open Project Config
        if os.path.exists(self.table_config_filename):
            with open(self.table_config_filename, "r", encoding="utf-8") as f:
                project_config = json.load(f)
                for config in project_config.get("items", []):
                    try:
                        table_config = TableConfig(**config)
                        self.add_table_config(table_config)
                    except Exception as e:
                        raise ConfigException(
                            f"Failed to load table config: {e}"
                        ) from e
        else:
            Path(self.project_directory).mkdir(parents=True, exist_ok=True)
            with open(self.table_config_filename, "w", encoding="utf-8") as f:
                json.dump({**self.base_config, "items": []}, f, indent=4)
        
        # Open Project Table Index
        if os.path.exists(self.table_index_filename):
            with open(self.table_index_filename, "r", encoding="utf-8") as f:
                self._project_table_index = json.load(f)
        else:
            # Ensure Directory exists
            index_dir = os.path.dirname(self.table_index_filename)
            if index_dir:
                Path(index_dir).mkdir(parents=True, exist_ok=True)
            
            # Save Base Project Table Index
            with open(self.table_index_filename, "w", encoding="utf-8") as f:
                json.dump({}, f, indent=4)
    
    def get_source_engine(self) -> MsSqlSourceEngine:
        """Get the source engine instance based on configuration."""
        if self.source_engine == SourceMssqlEngine:
            return MsSqlSourceEngine(**self.source_engine_params)
        raise NotImplementedError(f"Engine Type `{self.source_engine}` not implemented")
    
    def get_target_engine(self) -> FileSystemTargetEngine:
        """Get the target engine instance based on configuration."""
        if self.target_engine == TargetFileSystemEngine:
            return FileSystemTargetEngine(**self.target_engine_params)
        raise NotImplementedError(f"Engine Type `{self.target_engine}` not implemented")
    
    def save(self) -> None:
        """Save project configuration to files."""
        Path(self.project_directory).mkdir(parents=True, exist_ok=True)
        with open(self.table_config_filename, "w", encoding="utf-8") as f:
            json.dump(
                {
                    **self.base_config,
                    "items": [table.model_dump(exclude_none=False) for table in self.tables_config]
                },
                f,
                indent=4
            )
        
        with open(self.table_index_filename, "w", encoding="utf-8") as f:
            json.dump(self.project_table_index, f, default=meta_handler, indent=4)
    
    def add_table_config(self, table_config: TableConfig) -> None:
        """Add a table configuration to the project."""
        self._tables_config.append(table_config)
    
    def __str__(self) -> str:
        """String representation of the project config."""
        return str(self.name)
    
    class Config:
        """Pydantic v2 configuration."""
        arbitrary_types_allowed = True
        json_schema_extra = {
            "example": {
                "name": "AdventureWorks",
                "source_engine": "mssql",
                "source_engine_params": {
                    "connection_str": "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;..."
                },
                "target_engine": "file_system",
                "target_engine_params": {
                    "base_directory": "data/AdventureWorks",
                    "compression": "gzip"
                },
                "config_dir": "config"
            }
        }
