from yadet.objects.base import BaseObject

from typing import Dict


class TableConfig(BaseObject):
    
    def __init__(self, 
                 ordinal: int, 
                 table_name: str,
                 delta: bool,
                 order_by_columns: Dict[str, str],
                 columns: str = "*",
                 filter_clause: str = None,
                 join_clause: str = None,
                 batch_size: int = 5000,
                 active: bool = True,
                 desc: str = None):
        self.ordinal: int = ordinal
        self.table_name: str = table_name
        self.delta: bool = delta
        self.order_by_columns: Dict[str, str] = order_by_columns
        self.columns: str = columns
        self.filter_clause: str = filter_clause
        self.join_clause: str = join_clause
        self.batch_size: int =batch_size
        self.active: bool = active
        self.desc: str = desc

        self.validate()
        
    def validate(self):
        # TODO: Validate structure of elements
        # TODO: Validate Order by Columns
        ...
    
    @property
    def data(self) -> Dict:
        return {
            "ordinal": self.ordinal,
            "table_name": self.table_name,
            "delta": self.delta,
            "order_by_columns": self.order_by_columns,
            "columns": self.columns,
            "filter_clause": self.filter_clause,
            "join_clause": self.join_clause,
            "batch_size": self.batch_size,
            "active": self.active,
            "desc": self.desc,
        }
        
    def __str__(self) -> str:
        return f"{str(self.table_name)}: {self.ordinal}"