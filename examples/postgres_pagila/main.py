#!/usr/bin/env python
# coding: utf-8
# pip install yadet[postgres] --upgrade
from yadet.config import ProjectConfig
from yadet.batch import ProjectBatchRun
from yadet.engine import SourcePostgresEngine, TargetFileEventStoreEngine

import os

conn_str = "postgresql://postgres:postgres@localhost:5433/postgres"

project = ProjectConfig(
    name="Pagila",
    source_engine=SourcePostgresEngine,
    source_engine_params={"connection_str": conn_str, "debug": True},
    target_engine=TargetFileEventStoreEngine,
    target_engine_params={
        "base_directory": os.path.join("data", "Pagila"),
        # "compression": "gzip",
    },
)

batch_run = ProjectBatchRun(project=project, debug=True)
batch_run.run_batch()
