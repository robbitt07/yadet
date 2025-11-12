from yadet.config.table import TableConfig
from yadet.errors import ConfigException
from yadet.objects.base import BaseObject

from typing import Any

SourceMssqlEngine = "mssql"

SOURCE_ENGINE_OPTIONS = {SourceMssqlEngine, }


class SourceEngine(BaseObject):

    def __init__(self, vendor: str, debug: bool = False) -> None:
        self.vendor: str = vendor
        self.debug: bool = debug
        self._conn: Any = None
        self.validate()

    def validate(self):
        if self.vendor not in SOURCE_ENGINE_OPTIONS:
            raise ConfigException(
                f"Engine vendor=`{self.vendor}` not available, current options are `{', '.join(SOURCE_ENGINE_OPTIONS)}`"
            )
        if not hasattr(self, "connect"):
            raise ConfigException("Engine must have `connect` method")

    @property
    def conn(self) -> Any:
        return self._conn

    @property
    def active(self) -> bool:
        return self._conn is not None

    def close(self) -> None:
        """Close the database connection if it exists."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass  # Ignore errors during cleanup
            finally:
                self._conn = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection is closed."""
        self.close()
        return False  # Don't suppress exceptions

    def where_clause(self, table_config: TableConfig) -> str:
        raise NotImplementedError()

    def order_by_clause(self, table_config: TableConfig) -> str:
        raise NotImplementedError()

    def num_records_query(self, table_config: TableConfig) -> str:
        raise NotImplementedError()

    def extract_query(self, table_config: TableConfig, start_indx: int) -> str:
        raise NotImplementedError()

    def __str__(self) -> str:
        return str(self.vendor)
