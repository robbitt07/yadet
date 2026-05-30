from yadet.engine.target.base import TargetFileEventStoreEngine, TargetEngine
from yadet.helpers.json import meta_handler

import gzip
import json
import os
import uuid
from pathlib import Path
from typing import Dict, List


class FileEventStoreTargetEngine(TargetEngine):

    def __init__(self, base_directory: str, compression: str = None):
        super().__init__(vendor=TargetFileEventStoreEngine)
        self.base_directory = base_directory
        self.compression = compression

    def _safe_table_dir(self, table_name: str) -> str:
        return table_name.replace("/", "_")

    def table_directory(self, table_name: str) -> os.PathLike:
        return os.path.join(self.base_directory, self._safe_table_dir(table_name))

    def meta_directory(self) -> os.PathLike:
        return os.path.join(self.base_directory, "_meta")

    def write_batch(self, table_name: str, data: List[Dict], batch_key: str, start_indx: int):
        table_directory = self.table_directory(table_name)
        Path(table_directory).mkdir(parents=True, exist_ok=True)

        for record in data:
            event_id = uuid.uuid4()
            filename = f"{table_name}_{event_id}"

            if self.compression is None:
                with open(os.path.join(table_directory, f"{filename}.json"), "w") as f:
                    json.dump(record, f)

            elif self.compression == "gzip":
                with gzip.open(os.path.join(table_directory, f"{filename}.json.gz"), "w") as f:
                    f.write(json.dumps(record).encode("utf-8"))

            else:
                raise NotImplementedError(f"Compression=`{self.compression}` not available")

    def write_meta(self, meta: Dict, batch_key: str):
        meta_directory = self.meta_directory()
        Path(meta_directory).mkdir(parents=True, exist_ok=True)

        with open(os.path.join(meta_directory, f"{batch_key}.json"), "w") as f:
            json.dump(meta, f, default=meta_handler, indent=4)
