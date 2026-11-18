-- The rest of the first 60 days, once curated.productos has `marca`: the products from `brand_from` on, then the
-- marts and the orders as they stood at `upto`.
insert into curated.productos
select producto_id, nombre, categoria, precio, marca
from raw.productos where dt >= '${hivevar:brand_from}' and dt <= '${hivevar:upto}';

insert overwrite table marts.ventas_diarias
select v.dt, p.categoria, count(*), sum(v.cantidad), round(sum(v.importe), 2)
from curated.ventas v join curated.productos p on v.producto_id = p.producto_id
where v.dt <= '${hivevar:upto}'
group by v.dt, p.categoria;

insert overwrite table marts.top_clientes
select cliente_id, nombre, pedidos, importe, puesto from (
  select v.cliente_id, c.nombre, count(*) as pedidos, round(sum(v.importe), 2) as importe,
         row_number() over (order by sum(v.importe) desc) as puesto
  from curated.ventas v join curated.clientes c on v.cliente_id = c.cliente_id
  where v.dt > date_sub('${hivevar:upto}', 30) and v.dt <= '${hivevar:upto}'
  group by v.cliente_id, c.nombre
) ranked
where puesto <= 100;

insert into ops.pedidos
select evento_id, cliente_id, round(cantidad * precio, 2), 'nuevo', to_date(ts), to_date(ts)
from raw.eventos where dt <= '${hivevar:upto}' and tipo = 'compra';

update ops.pedidos set estado = 'enviado', actualizado = date '${hivevar:upto}'
where estado = 'nuevo' and creado <= date_sub('${hivevar:upto}', 2);

update ops.pedidos set estado = 'entregado', actualizado = date '${hivevar:upto}'
where estado = 'enviado' and creado <= date_sub('${hivevar:upto}', 5);
