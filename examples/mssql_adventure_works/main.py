#!/usr/bin/env python
# coding: utf-8
# pip install yadet --upgrade
from yadet.config import ProjectConfig
from yadet.batch import ProjectBatchRun
from yadet.engine import SourceMssqlEngine, TargetFileSystemEngine

import os

ServerName = "localhost"
MSQLDatabase = "AdventureWorksLT2019"

conn_str = "DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={0}; database={1};\
    TrustServerCertificate=yes;Trusted_Connection=yes;\
    ApplicationIntent=ReadOnly".format(ServerName, MSQLDatabase)

project = ProjectConfig(
    name="AdventureWorks", 
    source_engine=SourceMssqlEngine, 
    source_engine_params={"connection_str": conn_str, "debug": True}, 
    target_engine=TargetFileSystemEngine, 
    target_engine_params={
        "base_directory": os.path.join("data", "AdventureWorks"), "compression": "gzip"
    }
)

batch_run = ProjectBatchRun(project=project, debug=True)
batch_run.run_batch()