import importlib.util
import warnings

SOURCE_INSTALL_HINT = (
    "yadet has no source database driver installed. "
    "Install one with: pip install yadet[mssql], pip install yadet[postgres], "
    "or pip install yadet[all]"
)


def has_mssql_driver() -> bool:
    return importlib.util.find_spec("pyodbc") is not None


def has_postgres_driver() -> bool:
    return importlib.util.find_spec("psycopg") is not None


def has_any_source_driver() -> bool:
    return has_mssql_driver() or has_postgres_driver()


def warn_if_no_source_driver() -> None:
    if not has_any_source_driver():
        warnings.warn(SOURCE_INSTALL_HINT, UserWarning, stacklevel=2)
