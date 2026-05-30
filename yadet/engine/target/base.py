from typing import Dict, List

from yadet.objects.base import BaseObject

TargetFileSystemEngine = "file_system"
TargetFileEventStoreEngine = "file_event_store"

TARGET_ENGINE_OPTIONS = {TargetFileSystemEngine, TargetFileEventStoreEngine, }


class TargetEngine(BaseObject):

    def __init__(self, vendor: str) -> None:
        self.vendor: str = vendor

    def write_batch(self, table_name: str, data: List[Dict], batch_key: str, start_indx: int):
        raise NotImplementedError()

    def write_meta(self, meta: Dict, batch_key: str):
        raise NotImplementedError()

    def __str__(self):
        return str(self.vendor)
