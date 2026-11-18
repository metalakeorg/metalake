-- One drop of the web's events, `dt` and `hora`, refined: its purchases appended to curated.ventas, in the partition
-- of their day. One insert, one file: the four drops of a day are four files, as a production Hive leaves them.
insert into curated.ventas partition (dt)
select evento_id, ts, cliente_id, producto_id, cantidad, precio, round(cantidad * precio, 2), dt
from raw.eventos
where dt = '${hivevar:dt}' and hora = '${hivevar:hora}' and tipo = 'compra';
