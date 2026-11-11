"""Engines connected to the lake, each from its profile: `lake.iceberg.pyiceberg()` and the others call these.

Each engine is a library of its own, imported only when it is asked for: MetaLake installs none. What is handed back is
the engine's own object, so what one knows of each keeps working.
"""


def _engine(module, method, extra):
    """The engine's module, or an error that says what to install."""
    import importlib

    try:
        return importlib.import_module(module)
    except ImportError as missing:
        raise ImportError(
            f"lake.iceberg.{method}() needs {module}: pip install 'metalake[{extra}]'"
        ) from missing


def pyiceberg(profile):
    """A PyIceberg catalog of the lake, by the profile's name."""
    catalog = _engine("pyiceberg.catalog", "pyiceberg", "pyiceberg")
    return catalog.load_catalog(profile.name, **profile.properties)


def duckdb(profile, connection):
    """`connection`, or a new DuckDB connection, with the lake attached."""
    if connection is None:
        connection = _engine("duckdb", "duckdb", "duckdb").connect()
    for statement in profile.statements:
        connection.execute(statement)
    return connection
