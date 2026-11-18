-- Sales, as a Hive warehouse keeps them: Parquet files on HDFS, one directory per day. Hive writes no field ids;
-- `dt` lives only in the directories' names.
create database if not exists tienda;

create table if not exists tienda.ventas (
  id bigint,
  cliente string,
  importe double
)
partitioned by (dt string)
stored as parquet;

insert into tienda.ventas partition (dt = '2024-01-02') values
  (1, 'ana', 10.5),
  (2, 'luis', 20.0),
  (3, 'ana', 7.25);

insert into tienda.ventas partition (dt = '2024-01-03') values
  (4, 'marta', 99.9),
  (5, 'luis', 5.0);

select dt, count(*) as n, sum(importe) as total from tienda.ventas group by dt order by dt;
