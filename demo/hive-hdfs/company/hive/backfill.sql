-- The first 60 days, up to `upto`, loaded at once as a warehouse's history is, yet laid out as daily loads would
-- have: one insert per drop hour over every day, so each day's partition still holds four files. Products before
-- `brand_from` are loaded, the column `marca` added (alter.sql runs between the two halves), then the rest.

insert into curated.ventas partition (dt)
select evento_id, ts, cliente_id, producto_id, cantidad, precio, round(cantidad * precio, 2), dt
from raw.eventos where dt <= '${hivevar:upto}' and hora = '00' and tipo = 'compra';
insert into curated.ventas partition (dt)
select evento_id, ts, cliente_id, producto_id, cantidad, precio, round(cantidad * precio, 2), dt
from raw.eventos where dt <= '${hivevar:upto}' and hora = '06' and tipo = 'compra';
insert into curated.ventas partition (dt)
select evento_id, ts, cliente_id, producto_id, cantidad, precio, round(cantidad * precio, 2), dt
from raw.eventos where dt <= '${hivevar:upto}' and hora = '12' and tipo = 'compra';
insert into curated.ventas partition (dt)
select evento_id, ts, cliente_id, producto_id, cantidad, precio, round(cantidad * precio, 2), dt
from raw.eventos where dt <= '${hivevar:upto}' and hora = '18' and tipo = 'compra';

insert into curated.inventario partition (dt, almacen)
select producto_id, stock, dt, if(almacen = '', null, almacen)
from raw.stock where dt <= '${hivevar:upto}';

insert into curated.clientes
select cliente_id, nombre, segmento, pais, alta from raw.clientes where dt <= '${hivevar:upto}';

insert into curated.productos
select producto_id, nombre, categoria, precio from raw.productos where dt < '${hivevar:brand_from}';
