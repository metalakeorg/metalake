-- The close of day `dt`, after its four drops: the day's stock, the new customers and products, the marts rebuilt
-- whole, and the orders of the application's transactional table moved on. `marca` is `, marca` from day 45, when
-- curated.productos has the column (alter.sql), and empty before.

-- Stock per warehouse; a count with no warehouse goes to Hive's null partition, __HIVE_DEFAULT_PARTITION__.
insert into curated.inventario partition (dt = '${hivevar:dt}', almacen)
select producto_id, stock, if(almacen = '', null, almacen)
from raw.stock
where dt = '${hivevar:dt}';

insert into curated.clientes
select cliente_id, nombre, segmento, pais, alta from raw.clientes where dt = '${hivevar:dt}';

insert into curated.productos
select producto_id, nombre, categoria, precio ${hivevar:marca} from raw.productos where dt = '${hivevar:dt}';

-- The marts, rebuilt whole: every day's files replace the day before's.
insert overwrite table marts.ventas_diarias
select v.dt, p.categoria, count(*), sum(v.cantidad), round(sum(v.importe), 2)
from curated.ventas v join curated.productos p on v.producto_id = p.producto_id
where v.dt <= '${hivevar:dt}'
group by v.dt, p.categoria;

insert overwrite table marts.top_clientes
select cliente_id, nombre, pedidos, importe, puesto from (
  select v.cliente_id, c.nombre, count(*) as pedidos, round(sum(v.importe), 2) as importe,
         row_number() over (order by sum(v.importe) desc) as puesto
  from curated.ventas v join curated.clientes c on v.cliente_id = c.cliente_id
  where v.dt > date_sub('${hivevar:dt}', 30) and v.dt <= '${hivevar:dt}'
  group by v.cliente_id, c.nombre
) ranked
where puesto <= 100;

-- The application's orders: the day's are new; two days on they ship, five days on they are delivered.
insert into ops.pedidos
select evento_id, cliente_id, round(cantidad * precio, 2), 'nuevo', to_date(ts), date '${hivevar:dt}'
from raw.eventos
where dt = '${hivevar:dt}' and tipo = 'compra';

update ops.pedidos set estado = 'enviado', actualizado = date '${hivevar:dt}'
where estado = 'nuevo' and creado <= date_sub('${hivevar:dt}', 2);

update ops.pedidos set estado = 'entregado', actualizado = date '${hivevar:dt}'
where estado = 'enviado' and creado <= date_sub('${hivevar:dt}', 5);
