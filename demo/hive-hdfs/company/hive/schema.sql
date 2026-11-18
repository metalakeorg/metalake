-- Tienda's warehouse: three databases, as a Hadoop shop lays one out. `raw` is the landing zone, CSV the systems
-- drop; `curated` and `marts` are what Hive's ETL makes of it; `ops` is an application's transactional table.
-- Every table but `ops.pedidos` is external, as Hive 4 makes a managed table that is not transactional.

create database if not exists raw;
create database if not exists curated;
create database if not exists marts;
create database if not exists ops;

-- The landing zone: CSV with no header, one directory per partition, which `land.sh` puts and adds.
create external table if not exists raw.eventos (
  evento_id string, ts timestamp, cliente_id bigint, tipo string, producto_id bigint, cantidad int, precio double
)
partitioned by (dt string, hora string)
row format delimited fields terminated by ','
stored as textfile
location 'hdfs://127.0.0.1:9000/landing/eventos';

create external table if not exists raw.clientes (
  cliente_id bigint, nombre string, segmento string, pais string, alta date
)
partitioned by (dt string)
row format delimited fields terminated by ','
stored as textfile
location 'hdfs://127.0.0.1:9000/landing/clientes';

create external table if not exists raw.productos (
  producto_id bigint, nombre string, categoria string, precio double, marca string
)
partitioned by (dt string)
row format delimited fields terminated by ','
stored as textfile
location 'hdfs://127.0.0.1:9000/landing/productos';

create external table if not exists raw.stock (
  producto_id bigint, almacen string, stock int
)
partitioned by (dt string)
row format delimited fields terminated by ','
stored as textfile
location 'hdfs://127.0.0.1:9000/landing/stock';

-- Curated: Parquet, written by the ETL.
create external table if not exists curated.ventas (
  pedido_id string, ts timestamp, cliente_id bigint, producto_id bigint, cantidad int, precio double, importe double
)
partitioned by (dt string)
stored as parquet;

create external table if not exists curated.clientes (
  cliente_id bigint, nombre string, segmento string, pais string, alta date
)
stored as parquet;

-- `marca` is added on day 45 (alter.sql): the files before it have no such column.
create external table if not exists curated.productos (
  producto_id bigint, nombre string, categoria string, precio double
)
stored as parquet;

create external table if not exists curated.inventario (
  producto_id bigint, stock int
)
partitioned by (dt string, almacen string)
stored as parquet;

-- Marts: rebuilt whole every day.
create external table if not exists marts.ventas_diarias (
  dt string, categoria string, pedidos bigint, unidades bigint, importe double
)
stored as parquet;

create external table if not exists marts.top_clientes (
  cliente_id bigint, nombre string, pedidos bigint, importe double, puesto int
)
stored as orc;

-- An application's orders, whose status changes: Hive's ACID, ORC with delta directories.
create table if not exists ops.pedidos (
  pedido_id string, cliente_id bigint, importe double, estado string, creado date, actualizado date
)
stored as orc
tblproperties ('transactional' = 'true');
