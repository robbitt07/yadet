from typing import Dict, List

from yadet.objects.base import BaseObject

TargetFileSystemEngine = "file_system"

TARGET_ENGINE_OPTIONS = {TargetFileSystemEngine, }


class TargetEngine(BaseObject):

    def __init__(self, vendor: str) -> None:
        self.vendor: str = vendor

    def write_batch(self, data: List[Dict]):
        raise NotImplementedError()

    def __str__(self):
        return str(self.vendor)
