# Example - Postgres Pagila

## Prerequisites

- Docker
- Python 3.8+
- `pip install yadet[postgres]` (or `pip install yadet[all]`)

## Setup

Start the Pagila sample database:

```bash
docker compose up -d
```

Wait until the container is healthy before running the extract.

The database listens on port **5433** on the host (to avoid conflicting with a local Postgres on 5432).

## Usage

Run main.py

```bash
python main.py
```

## Output

Extracted data is written to `data/Pagila/` as one gzip-compressed JSON file per record, organized by table (e.g. `data/Pagila/public.customer/public.customer_<uuid>.json.gz`). Batch run metadata is stored in `data/Pagila/_meta/`. Delta watermark state for incremental tables is stored in `data/Pagila/project_table_index.json`.
