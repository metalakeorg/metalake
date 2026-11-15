"""Engines connected to the lake, each from its profile: `lake.iceberg.pyiceberg()` and the others call these.

Each engine is a library of its own, imported only when it is asked for: MetaLake installs none. What is handed back is
the engine's own object, so what one knows of each keeps working.
"""

import warnings

from .errors import UsageError

SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
HADOOP_CLASSPATH = "HADOOP_CLASSPATH"


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


def spark(profile, session):
    """`session`, else the active SparkSession, else a local one started with Iceberg: connected to the lake."""
    pyspark = _engine("pyspark", "spark", "spark")
    _same_version("Spark", pyspark.__version__, profile, "iceberg-spark-runtime-")
    from pyspark.sql import SparkSession

    if session is None:
        session = SparkSession.getActiveSession()
    if session is None:
        builder = SparkSession.builder.appName("metalake")
        for key, value in {**profile.startup, **profile.properties}.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()
    _iceberg_in(session, profile)
    # Spark loads a catalog the first time it is named, from the session's configuration then.
    for key, value in profile.properties.items():
        session.conf.set(key, value)
    return session


def _iceberg_in(session, profile):
    """A running session takes a catalog, but not Iceberg's classes, which are fixed as it starts."""
    jvm = getattr(session, "_jvm", None)
    if jvm is None:
        # Spark Connect: the classpath is the server's, not ours to see.
        return
    try:
        jvm.org.apache.spark.util.Utils.classForName("org.apache.iceberg.spark.SparkCatalog", True, False)
    except Exception:
        start = "".join(f"\n    .config({key!r}, {value!r})" for key, value in profile.startup.items())
        raise UsageError(f"this SparkSession started without Iceberg; start it with:{start}") from None
    if SPARK_EXTENSIONS not in (session.conf.get("spark.sql.extensions", "") or ""):
        warnings.warn(
            f"MERGE INTO and Iceberg's procedures need spark.sql.extensions={SPARK_EXTENSIONS} as the session starts",
            stacklevel=3,
        )


def flink(profile, environment, streaming):
    """`environment`, or a new TableEnvironment, batch unless `streaming`, with the lake's catalog."""
    table = _engine("pyflink.table", "flink", "flink")
    import pyflink.version

    _same_version("Flink", pyflink.version.__version__, profile, "iceberg-flink-runtime-")
    _classpath_for(profile)
    if environment is None:
        mode = table.EnvironmentSettings.in_streaming_mode() if streaming else table.EnvironmentSettings.in_batch_mode()
        environment = table.TableEnvironment.create(mode)
    _classpath_in(profile)
    for statement in profile.statements:
        environment.execute_sql(statement).wait()
    return environment


# What the lake's catalog needs on the classpath Flink started with: Iceberg's REST catalog, and Hadoop's client.
FLINK_CLASSES = ("org.apache.iceberg.rest.RESTCatalog", "org.apache.hadoop.conf.Configuration")


def _classpath_for(profile):
    """The lake's jars on the classpath PyFlink's JVM starts with, if it has not started: Flink loads Hadoop's classes,
    and Iceberg its catalog, from there alone, whatever jars it is given later."""
    import os

    from pyflink import java_gateway

    if java_gateway._gateway is not None:
        return
    held = os.environ.get(HADOOP_CLASSPATH, "")
    wanted = profile.startup[HADOOP_CLASSPATH]
    if wanted not in held:
        os.environ[HADOOP_CLASSPATH] = os.pathsep.join(part for part in (held, wanted) if part)


def _classpath_in(profile):
    """A Flink that started without the lake's jars cannot take them: it is refused, with what it needs at start."""
    from pyflink.java_gateway import get_gateway

    jvm = get_gateway().jvm
    for name in FLINK_CLASSES:
        try:
            jvm.java.lang.Class.forName(name)
        except Exception:
            raise UsageError(
                f"this Flink started without the lake's jars ({name}); start it with "
                f"{HADOOP_CLASSPATH}={profile.startup[HADOOP_CLASSPATH]}"
            ) from None


def _same_version(engine, installed, profile, runtime):
    """The engine's minor version is the one the Iceberg runtime the profile names was built for."""
    named = next(artifact for artifact in profile.artifacts if runtime in artifact)
    built = named.split(runtime)[1].split("_")[0].split(":")[0]
    if not installed.startswith(built + "."):
        raise UsageError(f"{engine} {installed} is installed; the lake's Iceberg runtime is for {engine} {built}")


# `USING duckdb …` from any face of this process: a connection per lake, made as `lake.iceberg.duckdb()` makes one and
# kept for the statements after it. One statement at a time on it, as DuckDB's connection is one.
_local = {}
_local_lock = None


def run_local(engine, setup, sql):
    """One statement on a local engine, connected by `setup` the first time: its rows, as Arrow."""
    import threading

    global _local_lock
    if _local_lock is None:
        _local_lock = threading.Lock()
    if engine != "duckdb":
        raise UsageError(f"USING {engine}: the engines that run here are duckdb")
    duckdb_module = _using("duckdb")
    pyarrow = _using("pyarrow")
    with _local_lock:
        key = tuple(setup)
        connection = _local.get(key)
        if connection is None:

            class Profile:
                statements = setup

            connection = duckdb(Profile, duckdb_module.connect())
            _local[key] = connection
        # `sql` runs a statement that answers no rows, as a CREATE or an INSERT, at once, and answers None for it.
        relation = connection.sql(sql)
        if relation is None:
            return pyarrow.table({})
        return _arrow_of(relation)


def _arrow_of(relation):
    """A DuckDB relation's rows as Arrow: by Arrow's own stream interface, else by the name the installed DuckDB gives."""
    if hasattr(relation, "__arrow_c_stream__"):
        return relation
    for name in ("to_arrow_table", "fetch_arrow_table", "arrow"):
        if hasattr(relation, name):
            return getattr(relation, name)()
    raise UsageError("this DuckDB answers no Arrow: pip install -U \"metalake[duckdb]\"")


def _using(module):
    """A module `USING duckdb` needs, or an error that says what to install, in the Python this runs in."""
    import importlib
    import sys

    try:
        return importlib.import_module(module)
    except ImportError as missing:
        raise ImportError(
            f"USING duckdb needs {module}, which this metalake's Python does not have: "
            f"{sys.executable} -m pip install \"metalake[duckdb]\""
        ) from missing
