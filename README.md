# YADET (Yet Another Data Extract Tool)

A simple and effective Python data extraction tool for getting batch data exports 
and store in a simple format.  

Current functionality is relatively limited with MsSQL and Postgres Source integration
and File System and File Event Store Target integration.

## Install 

With Python 3.8 or greater install the package with:

```
pip install yadet
```

Source database drivers are optional. Install the driver(s) you need:

```
pip install yadet[mssql]      # MsSQL (pyodbc)
pip install yadet[postgres]    # Postgres (psycopg)
pip install yadet[all]         # both source drivers
```

If you install `yadet` without a source extra, a warning is shown at install time and when importing the package.

## Usage


Import and load data
```python
from yadet.config import ProjectConfig
from yadet.batch import ProjectBatchRun
from yadet.engine import SourceMssqlEngine, TargetFileSystemEngine

import os

conn_str = "<>"

project = ProjectConfig(
    name="example", 
    source_engine=SourceMssqlEngine, 
    source_engine_params={"connection_str": conn_str}, 
    target_engine=TargetFileSystemEngine, 
    target_engine_params={
        "base_directory": os.path.join("data", "example"), 
        "compression": "gzip"
    }
)

batch_run = ProjectBatchRun(project=project, debug=True)
batch_run.run_batch()
```

For Postgres, use `SourcePostgresEngine` and a PostgreSQL connection string:

```python
from yadet.engine import SourcePostgresEngine, TargetFileSystemEngine

conn_str = "postgresql://user:pass@localhost:5432/dbname"

project = ProjectConfig(
    name="example",
    source_engine=SourcePostgresEngine,
    source_engine_params={"connection_str": conn_str},
    target_engine=TargetFileSystemEngine,
    target_engine_params={
        "base_directory": os.path.join("data", "example"), 
        "compression": "gzip"
    },
)

batch_run = ProjectBatchRun(project=project, debug=True)
batch_run.run_batch()
```

For a file event store target that writes one JSON file per record (`{table_name}_{uuid}.json`):

```python
from yadet.engine import SourcePostgresEngine, TargetFileEventStoreEngine

project = ProjectConfig(
    name="example",
    source_engine=SourcePostgresEngine,
    source_engine_params={"connection_str": conn_str},
    target_engine=TargetFileEventStoreEngine,
    target_engine_params={
        "base_directory": os.path.join("data", "example"),
        "compression": "gzip",  # optional
    },
)

batch_run = ProjectBatchRun(project=project, debug=True)
batch_run.run_batch()
```