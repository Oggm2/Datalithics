# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Objetivo
# MAGIC %md
# MAGIC # Pipeline Gold — Taco Bueno Pricing
# MAGIC
# MAGIC Crea las **6 tablas analíticas Gold** desde la Silver (`workspace.default.taco_bueno_silver`), respondiendo a los objetivos de negocio: efectividad de descuentos, estrategias de promoción, costo-beneficio y diferencias por producto, canal, fecha, día y hora.
# MAGIC
# MAGIC **Definiciones de métricas (corregidas en Fase 3 — montos a nivel check contados UNA vez por orden, consistentes en todas las tablas):**
# MAGIC * `total`, `total_discount`, `tax` y `order_voided` son montos **a nivel check repetidos en cada línea** de la Silver → se cuentan **UNA vez por orden**, con clave `store_id + date + check_number` (el `check_number` se reutiliza entre fechas de la misma tienda, por eso la clave incluye `date`)
# MAGIC * `transactions` = filas no-Void de la Silver (línea de transacción POS)
# MAGIC * `orders` = órdenes únicas (`store+date+check`)
# MAGIC * `revenue_gross` = Σ `MAX(total)` por orden (bruto, pre-descuento, sin impuesto); `revenue_net` = gross − descuento del check; `avg_order_value` = gross / orders
# MAGIC * `discount_amount_check` = Σ `MAX(total_discount)` por orden (descuento real contado una vez); `discount_amount_item` = Σ `item_discount` (nivel ítem)
# MAGIC * `pct_orders_with_discount` = % de órdenes con descuento (nivel orden); `pct_lines_in_discounted_check` = % de líneas dentro de checks descontados
# MAGIC * **Producto:** métricas a nivel ítem (`gross_item_sales` = Σ `menu_price×quantity`, `item_discount_amount` = Σ `item_discount`). El descuento a nivel check **NO se atribuye a productos** (pertenece al check completo)
# MAGIC
# MAGIC **Caveats documentados:**
# MAGIC * `total`/`total_discount` SON montos a nivel check repetidos por línea (verificado: 0 de 21,638 checks multi-línea de un día muestreado varían entre líneas) → toda `SUM(total)` por línea multiplica el check por su número de líneas (~×6). Las tablas Gold ahora los cuentan UNA vez por orden.
# MAGIC * Todas las tablas excluyen `transaction_type = 'Void'` (base: 64,862,789 filas).
# MAGIC * Las comparaciones con/sin descuento son **descriptivas, NO causales** (sin grupo de control).
# MAGIC * `tender_type` (87.6% NULL) no se usa como dimensión; `report_group` (~29% NULL) solo como atributo de producto.
# MAGIC
# MAGIC Al final: DQ checks de la capa Gold con reconciliación cruzada contra la Silver, persistidos en `workspace.default.taco_bueno_dq_results`.

# COMMAND ----------

# DBTITLE 1,Doc — gold_monthly
# MAGIC %md
# MAGIC ## Gold 1 — `taco_bueno_gold_monthly`
# MAGIC
# MAGIC * **Propósito:** tendencia mensual de revenue, descuentos, transacciones y ticket.
# MAGIC * **Grain:** una fila = un mes calendario.
# MAGIC * **Uso downstream:** dashboard de tendencias, análisis de tráfico vs descuento, detección de anomalías mensuales.
# MAGIC * **Fase 3:** `revenue_gross`/`revenue_net`/`discount_amount_check` a nivel ORDEN (una vez por check); `discount_amount_item` = Σ `item_discount`; `pct_orders_with_discount` = % de órdenes con descuento.

# COMMAND ----------

# DBTITLE 1,GOLD 1 — monthly
# GOLD 1 — Métricas mensuales
# Fase 3: montos a nivel CHECK contados UNA vez por orden (clave store+date+check).
# En la Silver, `total`/`total_discount` repiten el monto del check en cada línea;
# sumarlos por línea infla los montos ×~6. Aquí: una fila por orden vía MAX().
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_monthly AS
WITH checks AS (
    SELECT store_id, date, check_number,
           MAX(total) AS check_total,
           MAX(total_discount) AS check_discount
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void'
    GROUP BY 1, 2, 3
),
chk AS (
    SELECT DATE_TRUNC('month', date) AS month,
           COUNT(*) AS orders,
           SUM(check_total) AS revenue_gross,
           SUM(check_total - check_discount) AS revenue_net,
           SUM(check_discount) AS discount_amount_check,
           SUM(CASE WHEN check_discount > 0 THEN 1 ELSE 0 END) AS orders_with_discount,
           COUNT(DISTINCT store_id) AS active_stores
    FROM checks
    GROUP BY 1
),
lines AS (
    SELECT DATE_TRUNC('month', date) AS month,
           COUNT(*) AS transactions,
           SUM(item_discount) AS discount_amount_item,
           SUM(CASE WHEN total_discount > 0 THEN 1 ELSE 0 END) AS lines_in_discounted_check
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void'
    GROUP BY 1
)
SELECT
    c.month AS month,
    l.transactions AS transactions,
    c.orders AS orders,
    ROUND(c.revenue_gross, 2) AS revenue_gross,
    ROUND(c.revenue_net, 2) AS revenue_net,
    ROUND(c.revenue_gross / c.orders, 2) AS avg_order_value,
    ROUND(c.discount_amount_check, 2) AS discount_amount_check,
    ROUND(l.discount_amount_item, 2) AS discount_amount_item,
    ROUND(c.orders_with_discount * 100.0 / c.orders, 2) AS pct_orders_with_discount,
    ROUND(l.lines_in_discounted_check * 100.0 / l.transactions, 2) AS pct_lines_in_discounted_check,
    c.active_stores AS active_stores
FROM chk c JOIN lines l ON c.month = l.month
ORDER BY 1
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_monthly").collect()[0]['n']
print(f"✅ taco_bueno_gold_monthly — {n} meses (montos a nivel check, una vez por orden)")
spark.sql("SELECT month, transactions, orders, revenue_gross, revenue_net, avg_order_value, pct_orders_with_discount FROM workspace.default.taco_bueno_gold_monthly ORDER BY month").show(25, truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_product
# MAGIC %md
# MAGIC ## Gold 2 — `taco_bueno_gold_product`
# MAGIC
# MAGIC * **Propósito:** desempeño y descuento por producto (incluye Steak Soft Taco del análisis Fase 1).
# MAGIC * **Grain:** una fila = un `menu_item` (excluye NULL y 'Modifier (NR)' se conserva con su propio caveat).
# MAGIC * **Fase 3:** métricas a nivel ÍTEM (`gross_item_sales` = Σ `menu_price × quantity`, `item_discount_amount` = Σ `item_discount`, `item_discount_per_unit` = descuento por unidad vendida). El descuento a nivel check NO se atribuye a productos.
# MAGIC * **Caveat:** excluye filas con `menu_item` NULL (documentado en DQ; el check `gold_product_cobertura` reporta cuántas).

# COMMAND ----------

# DBTITLE 1,GOLD 2 — product
# GOLD 2 — Desempeño por producto
# Fase 3: nivel ÍTEM. gross_item_sales = Σ(menu_price × quantity) y el descuento es
# Σ(item_discount). El descuento a nivel check NO se atribuye a productos: pertenece
# al check completo (se reporta en las tablas a nivel orden: monthly/channel).
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_product AS
SELECT
    menu_item,
    MAX(report_group) AS report_group,
    COUNT(*) AS transactions,
    SUM(quantity) AS units_sold,
    ROUND(SUM(menu_price * quantity), 2) AS gross_item_sales,
    ROUND(SUM(item_discount), 2) AS item_discount_amount,
    ROUND(SUM(menu_price * quantity - item_discount), 2) AS net_item_revenue,
    ROUND(SUM(CASE WHEN item_discount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_lines_item_discount,
    ROUND(SUM(CASE WHEN total_discount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_lines_in_discounted_check,
    ROUND(SUM(item_discount) / NULLIF(SUM(quantity), 0), 4) AS item_discount_per_unit
FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void'
  AND menu_item IS NOT NULL
GROUP BY menu_item
ORDER BY gross_item_sales DESC
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_product").collect()[0]['n']
print(f"✅ taco_bueno_gold_product — {n} productos (nivel ítem; sin atribución de descuento de check)")
spark.sql("""SELECT menu_item, transactions, units_sold, gross_item_sales, item_discount_amount, pct_lines_item_discount
FROM workspace.default.taco_bueno_gold_product
WHERE transactions >= 10000 ORDER BY pct_lines_item_discount DESC LIMIT 10""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_channel
# MAGIC %md
# MAGIC ## Gold 3 — `taco_bueno_gold_channel`
# MAGIC
# MAGIC * **Propósito:** volumen y descuento por canal de venta (`pop`): Drive Thru, To Go, Online: DoorDash, Mobile, etc.
# MAGIC * **Grain:** una fila = un canal (`pop`). Incluye el canal `3` (atributo POS adicional válido, decisión de Fase 1).
# MAGIC * **Fase 3:** `revenue_gross`/`revenue_net`/`discount_amount_check` a nivel ORDEN. Un check multicanal (58 en el dataset, monitoreados por `checks_canal_mixo`) se atribuye a su **canal dominante** (el canal con más líneas del check, empate roto por nombre) para que cada orden cuente UNA vez y la tabla reconcilie con `gold_monthly`; `transactions` y `discount_amount_item` (nivel línea) se cuentan en el canal real de cada línea.

# COMMAND ----------

# DBTITLE 1,GOLD 3 — channel
# GOLD 3 — Desempeño por canal
# Fase 3: montos a nivel CHECK una vez por orden. Un check multicanal (58 en el
# dataset; DQ: checks_canal_mixo) se atribuye a su canal DOMINANTE (más líneas,
# empate por nombre) para que cada orden cuente una sola vez y la tabla
# reconcilie con gold_monthly. `transactions` y `discount_amount_item` (nivel
# línea) se quedan en el canal real de cada línea.
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_channel AS
WITH chan_lines AS (
    SELECT store_id, date, check_number, pop, COUNT(*) AS n_lines
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void'
    GROUP BY 1, 2, 3, 4
),
dominant AS (
    SELECT store_id, date, check_number, pop AS dom_pop
    FROM (
        SELECT store_id, date, check_number, pop,
               ROW_NUMBER() OVER (
                   PARTITION BY store_id, date, check_number
                   ORDER BY n_lines DESC, pop
               ) AS rn
        FROM chan_lines
    )
    WHERE rn = 1
),
checks AS (
    SELECT s.store_id, s.date, s.check_number, d.dom_pop AS pop,
           MAX(s.total) AS check_total,
           MAX(s.total_discount) AS check_discount
    FROM workspace.default.taco_bueno_silver s
    JOIN dominant d
      ON s.store_id = d.store_id AND s.date = d.date AND s.check_number = d.check_number
    WHERE s.transaction_type != 'Void'
    GROUP BY 1, 2, 3, 4
),
chk AS (
    SELECT pop,
           COUNT(*) AS orders,
           SUM(check_total) AS revenue_gross,
           SUM(check_total - check_discount) AS revenue_net,
           SUM(check_discount) AS discount_amount_check,
           SUM(CASE WHEN check_discount > 0 THEN 1 ELSE 0 END) AS orders_with_discount,
           COUNT(DISTINCT store_id) AS active_stores
    FROM checks
    GROUP BY pop
),
lines AS (
    SELECT pop,
           COUNT(*) AS transactions,
           SUM(item_discount) AS discount_amount_item
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void'
    GROUP BY pop
)
SELECT
    l.pop AS channel,
    l.transactions AS transactions,
    c.orders AS orders,
    ROUND(c.revenue_gross, 2) AS revenue_gross,
    ROUND(c.revenue_net, 2) AS revenue_net,
    ROUND(c.revenue_gross / c.orders, 2) AS avg_order_value,
    ROUND(c.discount_amount_check, 2) AS discount_amount_check,
    ROUND(l.discount_amount_item, 2) AS discount_amount_item,
    ROUND(c.orders_with_discount * 100.0 / c.orders, 2) AS pct_orders_with_discount,
    c.active_stores AS active_stores
FROM chk c JOIN lines l ON c.pop = l.pop
ORDER BY l.transactions DESC
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_channel").collect()[0]['n']
print(f"✅ taco_bueno_gold_channel — {n} canales (montos a nivel check, una vez por orden)")
spark.sql("SELECT * FROM workspace.default.taco_bueno_gold_channel").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_hourly
# MAGIC %md
# MAGIC ## Gold 4 — `taco_bueno_gold_hourly`
# MAGIC
# MAGIC * **Propósito:** ventas por hora del día — base para propuestas de promociones de tarde (Fase 1).
# MAGIC * **Grain:** una fila = una hora del día (0–23), agregando todos los días y tiendas.
# MAGIC * **Caveat:** la hora viene de `begin_time` (solo hora, sin fecha real del evento); 0 filas con hora nula según DQ de la Silver.
# MAGIC * **Fase 3:** hora del check = `HOUR(MAX(begin_time))` y montos a nivel ORDEN — los absolutos ya NO están inflados ×~6 (antes solo las participaciones % eran válidas).

# COMMAND ----------

# DBTITLE 1,GOLD 4 — hourly
# GOLD 4 — Ventas por hora del día
# Fase 3: hora del check = HOUR(MAX(begin_time)); montos a nivel CHECK una vez por orden.
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_hourly AS
WITH checks AS (
    SELECT store_id, date, check_number,
           HOUR(MAX(begin_time)) AS hour_of_day,
           MAX(total) AS check_total,
           MAX(total_discount) AS check_discount
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void' AND begin_time IS NOT NULL
    GROUP BY 1, 2, 3
),
agg AS (
    SELECT hour_of_day,
           COUNT(*) AS orders,
           SUM(check_total) AS revenue_gross,
           SUM(check_discount) AS discount_amount_check,
           SUM(CASE WHEN check_discount > 0 THEN 1 ELSE 0 END) AS orders_with_discount
    FROM checks
    GROUP BY 1
),
lines AS (
    SELECT HOUR(begin_time) AS hour_of_day, COUNT(*) AS transactions
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void' AND begin_time IS NOT NULL
    GROUP BY 1
)
SELECT
    a.hour_of_day AS hour_of_day,
    l.transactions AS transactions,
    a.orders AS orders,
    ROUND(a.revenue_gross, 2) AS revenue_gross,
    ROUND(a.revenue_gross / a.orders, 2) AS avg_order_value,
    ROUND(a.discount_amount_check, 2) AS discount_amount_check,
    ROUND(a.orders_with_discount * 100.0 / a.orders, 2) AS pct_orders_with_discount
FROM agg a JOIN lines l ON a.hour_of_day = l.hour_of_day
ORDER BY 1
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_hourly").collect()[0]['n']
print(f"✅ taco_bueno_gold_hourly — {n} horas con ventas (montos a nivel check)")
spark.sql("SELECT * FROM workspace.default.taco_bueno_gold_hourly ORDER BY revenue_gross DESC LIMIT 8").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_dayofweek
# MAGIC %md
# MAGIC ## Gold 5 — `taco_bueno_gold_dayofweek`
# MAGIC
# MAGIC * **Propósito:** desempeño por día de la semana y fin de semana vs día laboral — base para propuestas de bundles de fin de semana (Fase 1).
# MAGIC * **Grain:** una fila = un día de la semana (1=domingo … 7=sábado, convención Spark).
# MAGIC * **Métrica clave:** `avg_orders_per_store_day` = órdenes promedio por tienda por día (misma métrica de tráfico de la EDA Fase 1).
# MAGIC * **Fase 3:** órdenes con clave store+date+check y `revenue_gross` a nivel ORDEN. Nota: `avg_orders_per_store_day` ya contaba el check una sola vez por tienda-día en la versión anterior, por eso el TRÁFICO no cambia; el revenue sí (antes inflado ×~6).

# COMMAND ----------

# DBTITLE 1,GOLD 5 — dayofweek
# GOLD 5 — Desempeño por día de la semana (con órdenes por tienda por día)
# Fase 3: órdenes con clave store+date+check (el check_number se reutiliza entre
# fechas) y montos a nivel CHECK una vez por orden.
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_dayofweek AS
WITH store_daily AS (
    SELECT
        date,
        DAYOFWEEK(date) AS dow,
        DATE_FORMAT(date, 'EEEE') AS day_name,
        store_id,
        COUNT(DISTINCT check_number) AS orders,
        COUNT(*) AS transactions,
        SUM(CASE WHEN total_discount > 0 THEN 1 ELSE 0 END) AS lines_in_discounted_check
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void'
    GROUP BY 1, 2, 3, 4
),
daily_checks AS (
    -- Montos del check UNA vez por orden (store+date+check)
    SELECT store_id, date,
           COUNT(*) AS orders_c,
           SUM(check_total) AS revenue_gross,
           SUM(check_total - check_discount) AS revenue_net,
           SUM(check_discount) AS discount_amount_check,
           SUM(CASE WHEN check_discount > 0 THEN 1 ELSE 0 END) AS orders_with_discount
    FROM (
        SELECT store_id, date, check_number,
               MAX(total) AS check_total,
               MAX(total_discount) AS check_discount
        FROM workspace.default.taco_bueno_silver
        WHERE transaction_type != 'Void'
        GROUP BY 1, 2, 3
    )
    GROUP BY 1, 2
)
SELECT
    s.dow AS day_of_week_num,
    s.day_name AS day_name,
    s.dow IN (1, 7) AS is_weekend,
    COUNT(*) AS store_days,
    SUM(s.transactions) AS transactions,
    SUM(s.orders) AS orders,
    ROUND(SUM(s.orders) * 1.0 / COUNT(*), 1) AS avg_orders_per_store_day,
    ROUND(SUM(c.revenue_gross), 2) AS revenue_gross,
    ROUND(SUM(c.revenue_net), 2) AS revenue_net,
    ROUND(SUM(c.discount_amount_check), 2) AS discount_amount_check,
    ROUND(SUM(c.orders_with_discount) * 100.0 / SUM(c.orders_c), 2) AS pct_orders_with_discount,
    ROUND(SUM(s.lines_in_discounted_check) * 100.0 / SUM(s.transactions), 2) AS pct_lines_in_discounted_check
FROM store_daily s
JOIN daily_checks c ON s.store_id = c.store_id AND s.date = c.date
GROUP BY s.dow, s.day_name
ORDER BY s.dow
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_dayofweek").collect()[0]['n']
print(f"✅ taco_bueno_gold_dayofweek — {n} días de la semana (montos a nivel check)")
spark.sql("SELECT * FROM workspace.default.taco_bueno_gold_dayofweek").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_promo_effectiveness
# MAGIC %md
# MAGIC ## Gold 6 — `taco_bueno_gold_promo_effectiveness`
# MAGIC
# MAGIC * **Propósito:** comparar transacciones con y sin descuento por mes (tasa, ticket, revenue) — la tabla de efectividad de promociones.
# MAGIC * **Grain:** una fila = mes × segmento (`Con descuento` / `Sin descuento`).
# MAGIC * **Caveat explícito:** comparación **descriptiva**. Sin grupo de control no puede afirmarse causalidad (el descuento podría correlacionarse con promociones estacionales). El costo del descuento se mide con `discount_amount_check`.
# MAGIC * **Fase 3:** grain por ORDEN (store+date+check): `orders`, `revenue_gross`, `avg_order_value` = check bruto promedio por orden, `discount_amount_check` contado UNA vez. La segmentación es a nivel orden (antes por línea).

# COMMAND ----------

# DBTITLE 1,GOLD 6 — promo effectiveness
# GOLD 6 — Efectividad de promociones: con vs sin descuento, por mes
# Fase 3: grain por ORDEN (store+date+check), montos contados UNA vez.
# Comparación DESCRIPTIVA (sin grupo de control): no prueba causalidad.
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_promo_effectiveness AS
WITH checks AS (
    SELECT store_id, date, check_number,
           MAX(total) AS check_total,
           MAX(total_discount) AS check_discount
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void'
    GROUP BY 1, 2, 3
)
SELECT
    DATE_TRUNC('month', date) AS month,
    CASE WHEN check_discount > 0 THEN 'Con descuento' ELSE 'Sin descuento' END AS segment,
    COUNT(*) AS orders,
    ROUND(SUM(check_total), 2) AS revenue_gross,
    ROUND(SUM(check_total - check_discount), 2) AS revenue_net,
    ROUND(SUM(check_total) / COUNT(*), 2) AS avg_order_value,
    ROUND(SUM(check_discount), 2) AS discount_amount_check,
    ROUND(SUM(check_total - check_discount) / NULLIF(SUM(check_discount), 0), 2) AS revenue_per_discount_dollar
FROM checks
GROUP BY 1, 2
ORDER BY 1, 2
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_promo_effectiveness").collect()[0]['n']
print(f"✅ taco_bueno_gold_promo_effectiveness — {n} filas (mes × segmento, nivel orden)")
spark.sql("""SELECT month, segment, orders, revenue_gross, avg_order_value, discount_amount_check
FROM workspace.default.taco_bueno_gold_promo_effectiveness
WHERE month >= '2026-07-01' ORDER BY month, segment""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — DQ Gold
# MAGIC %md
# MAGIC ## DQ Checks — capa Gold
# MAGIC
# MAGIC Reconciliación cruzada: cada tabla Gold debe reproducir los totales de la Silver no-Void (transacciones exactas; revenue dentro de una tolerancia de redondeo de $5). Excepción documentada: `gold_product` excluye filas con `menu_item` NULL y se reconcilia contra esa base. Resultados persistidos en `taco_bueno_dq_results` (layer = `gold`).

# COMMAND ----------

# DBTITLE 1,DQ Checks — Gold
from datetime import datetime, timezone

# ---------- Data Quality Checks: Gold (reconciliación cruzada con Silver, montos a nivel check) ----------
TOL = 5.0  # tolerancia por redondeo a 2 decimales por fila agregada

# Base no-Void a nivel ORDEN (clave store+date+check): montos del check contados UNA vez.
check_base = spark.sql("""
WITH checks AS (
    SELECT store_id, date, check_number,
           MAX(total) AS check_total,
           MAX(total_discount) AS check_discount
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void'
    GROUP BY 1, 2, 3
)
SELECT COUNT(*) AS orders,
       ROUND(SUM(check_total), 2) AS revenue_gross,
       ROUND(SUM(check_discount), 2) AS discount_check
FROM checks
""").collect()[0]

# Base no-Void a nivel LÍNEA con menú (reconciliación de gold_product, nivel ítem)
base_prod = spark.sql("""
SELECT COUNT(*) AS n, ROUND(SUM(menu_price * quantity), 2) AS item_gross
FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void' AND menu_item IS NOT NULL
""").collect()[0]

# Base hourly (excluye begin_time nulo, coherente con gold_hourly)
base_hourly = spark.sql("""
WITH checks AS (
    SELECT store_id, date, check_number, MAX(total) AS check_total
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void' AND begin_time IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT ROUND(SUM(check_total), 2) AS revenue_gross
FROM checks
""").collect()[0]

exp_months = spark.sql("""
SELECT COUNT(DISTINCT DATE_TRUNC('month', date)) AS m
FROM workspace.default.taco_bueno_silver WHERE transaction_type != 'Void'
""").collect()[0]['m']
exp_channels = spark.sql("""
SELECT COUNT(DISTINCT pop) AS c
FROM workspace.default.taco_bueno_silver WHERE transaction_type != 'Void'
""").collect()[0]['c']
exp_products = spark.sql("""
SELECT COUNT(DISTINCT menu_item) AS p
FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void' AND menu_item IS NOT NULL
""").collect()[0]['p']
null_menu = spark.sql("""
SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void' AND menu_item IS NULL
""").collect()[0]['n']

# Reglas estructurales: los montos del check DEBEN ser constantes dentro de cada orden
# (supuesto de las tablas Gold: MAX(total) toma el valor del check una sola vez).
consistency = spark.sql("""
SELECT
  SUM(CASE WHEN n_totals > 1 OR n_discs > 1 THEN 1 ELSE 0 END) AS inconsistent_checks,
  SUM(CASE WHEN n_pops > 1 THEN 1 ELSE 0 END) AS mixed_channel_checks
FROM (
  SELECT store_id, date, check_number,
         COUNT(DISTINCT total) AS n_totals,
         COUNT(DISTINCT total_discount) AS n_discs,
         COUNT(DISTINCT pop) AS n_pops
  FROM workspace.default.taco_bueno_silver
  WHERE transaction_type != 'Void'
  GROUP BY 1, 2, 3
)
""").collect()[0]

def agg(table):
    r = spark.sql(f"""SELECT COUNT(*) AS rows, SUM(transactions) AS trans, SUM(orders) AS ord,
                             ROUND(SUM(revenue_gross), 2) AS rev
                   FROM workspace.default.{table}""").collect()[0]
    return r['rows'], r['trans'], r['rev'], r['ord']

now = datetime.now(timezone.utc)
def mk(name, rule, observed, expected, ok, action, warn_only=False):
    status = "PASS" if ok else ("WARN" if warn_only else "FAIL")
    return (now, "gold", name, rule, str(observed), expected, status, action)

checks = []

# --- Reglas estructurales (nuevas en Fase 3) ---
checks.append(mk("montos_check_consistentes",
                 "total/total_discount constantes dentro de cada orden (supuesto del MAX por check)",
                 consistency['inconsistent_checks'], "0",
                 consistency['inconsistent_checks'] == 0,
                 "WARN: los montos varían dentro de un check — anomalía de la fuente (71 de 13.78M órdenes, 0.0005%); las tablas usan MAX(total) determinista; escalar a FAIL si crece", warn_only=True))
checks.append(mk("checks_canal_mixo",
                 "Órdenes con más de un canal (pop) — romperían el grain de gold_channel",
                 consistency['mixed_channel_checks'], "0",
                 consistency['mixed_channel_checks'] == 0,
                 "WARN: checks con canal mixto — gold_channel los atribuye a su canal dominante (más líneas), contando la orden UNA vez; las métricas de línea quedan en su canal real", warn_only=True))

# --- gold_monthly ---
rows, trans, rev, ord_ = agg("taco_bueno_gold_monthly")
checks.append(mk("gold_monthly_filas", "Filas == meses distintos en Silver", rows, exp_months,
                  rows == exp_months, "FAIL: meses faltantes o duplicados"))
checks.append(mk("gold_monthly_revenue", "SUM(revenue_gross) ≈ Silver a nivel check (tol $5)",
                  f"${rev:,.2f}", f"${check_base['revenue_gross']:,.2f}",
                  abs(rev - check_base['revenue_gross']) <= TOL,
                  "FAIL: la agregación mensual no reconcilia con la Silver"))
checks.append(mk("gold_monthly_orders",
                  "SUM(orders) == órdenes únicas en Silver (clave store+date+check; sin doble conteo entre meses)",
                  ord_, check_base['orders'], ord_ == check_base['orders'],
                  "FAIL: órdenes perdidas o duplicadas"))

# --- gold_channel ---
rows, trans, rev, ord_ = agg("taco_bueno_gold_channel")
checks.append(mk("gold_channel_filas", "Filas == canales (pop) distintos", rows, exp_channels,
                  rows == exp_channels, "FAIL: canal faltante"))
checks.append(mk("gold_channel_revenue", "SUM(revenue_gross) ≈ Silver a nivel check (tol $5)",
                  f"${rev:,.2f}", f"${check_base['revenue_gross']:,.2f}",
                  abs(rev - check_base['revenue_gross']) <= TOL,
                  "FAIL: la agregación por canal no reconcilia"))

# --- gold_hourly ---
rows, trans, rev, ord_ = agg("taco_bueno_gold_hourly")
checks.append(mk("gold_hourly_filas", "Filas == horas con ventas (esperado 24)", rows, "24",
                  rows == 24, "WARN: alguna hora sin ventas — verificar cobertura", warn_only=True))
checks.append(mk("gold_hourly_revenue", "SUM(revenue_gross) ≈ Silver a nivel check sin begin_time nulo (tol $5)",
                  f"${rev:,.2f}", f"${base_hourly['revenue_gross']:,.2f}",
                  abs(rev - base_hourly['revenue_gross']) <= TOL,
                  "FAIL: la agregación por hora no reconcilia"))

# --- gold_dayofweek ---
rows, trans, rev, ord_ = agg("taco_bueno_gold_dayofweek")
checks.append(mk("gold_dayofweek_filas", "Filas == 7 días de la semana", rows, "7",
                  rows == 7, "FAIL: día faltante"))
checks.append(mk("gold_dayofweek_revenue", "SUM(revenue_gross) ≈ Silver a nivel check (tol $5)",
                  f"${rev:,.2f}", f"${check_base['revenue_gross']:,.2f}",
                  abs(rev - check_base['revenue_gross']) <= TOL,
                  "FAIL: la agregación por día no reconcilia"))

# --- gold_product (base distinta: nivel ÍTEM, sin menu_item NULL — documentado) ---
r = spark.sql("""SELECT COUNT(*) AS rows, SUM(transactions) AS trans,
                       ROUND(SUM(gross_item_sales), 2) AS gross
                FROM workspace.default.taco_bueno_gold_product""").collect()[0]
checks.append(mk("gold_product_filas", "Filas == menu_items distintos no-NULL", r['rows'], exp_products,
                  r['rows'] == exp_products, "FAIL: producto faltante"))
checks.append(mk("gold_product_revenue", "SUM(gross_item_sales) ≈ Silver no-Void sin menu_item NULL, nivel ítem (tol $5)",
                  f"${r['gross']:,.2f}", f"${base_prod['item_gross']:,.2f}",
                  abs(r['gross'] - base_prod['item_gross']) <= TOL,
                  "FAIL: la agregación por producto no reconcilia con su base"))
checks.append(mk("gold_product_cobertura", "Filas NULL de menu_item excluidas (documentado)",
                  f"{null_menu:,} filas", "documentado", True,
                  "WARN: documentar la exclusión de menú NULL en el análisis", warn_only=True))

# --- gold_promo_effectiveness ---
r = spark.sql("""SELECT COUNT(*) AS rows, SUM(orders) AS ord, ROUND(SUM(revenue_gross), 2) AS rev
                FROM workspace.default.taco_bueno_gold_promo_effectiveness""").collect()[0]
checks.append(mk("gold_promo_filas", "Filas == 2 segmentos × meses", r['rows'], 2 * exp_months,
                  r['rows'] == 2 * exp_months, "FAIL: segmento o mes faltante"))
checks.append(mk("gold_promo_orders", "SUM(orders) == órdenes únicas en Silver (partición con/sin descuento completa)",
                  r['ord'], check_base['orders'], r['ord'] == check_base['orders'],
                  "FAIL: la partición con/sin descuento pierde o duplica órdenes"))
checks.append(mk("gold_promo_revenue", "SUM(revenue_gross) ≈ Silver a nivel check (tol $5)",
                  f"${r['rev']:,.2f}", f"${check_base['revenue_gross']:,.2f}",
                  abs(r['rev'] - check_base['revenue_gross']) <= TOL,
                  "FAIL: la partición con/sin descuento no reconcilia"))

# --- Identidad contable: gross - net == descuento (coherencia interna) ---
ident = spark.sql("""
SELECT ROUND(SUM(revenue_gross - revenue_net), 2) AS gross_minus_net,
       ROUND(SUM(discount_amount_check), 2) AS disc
FROM workspace.default.taco_bueno_gold_monthly
""").collect()[0]
checks.append(mk("identidad_gross_net_descuento",
                 "Σ(revenue_gross − revenue_net) == Σ(discount_amount_check) en gold_monthly",
                  f"${ident['gross_minus_net']:,.2f}", f"${ident['disc']:,.2f}",
                  abs(ident['gross_minus_net'] - ident['disc']) <= TOL,
                  "FAIL: la identidad contable gross−net=descuento no cuadra"))

# Persistir (delete+insert por capa = rerun seguro)
spark.sql("""CREATE TABLE IF NOT EXISTS workspace.default.taco_bueno_dq_results (
  checked_at TIMESTAMP, layer STRING, check_name STRING, rule STRING,
  observed STRING, expected STRING, status STRING, action STRING
)""")
spark.sql("DELETE FROM workspace.default.taco_bueno_dq_results WHERE layer = 'gold'")
dq = spark.createDataFrame(checks,
    schema="checked_at timestamp, layer string, check_name string, rule string, observed string, expected string, status string, action string")
dq.write.mode("append").saveAsTable("workspace.default.taco_bueno_dq_results")

n_fail = sum(1 for c in checks if c[6] == "FAIL")
n_warn = sum(1 for c in checks if c[6] == "WARN")
for c in checks:
    icon = "✅" if c[6] == "PASS" else ("⚠️" if c[6] == "WARN" else "❌")
    print(f"{icon} [{c[6]}] {c[2]}: observado={c[4]} (esperado: {c[5]})")
print(f"\nBase no-Void a nivel orden: {check_base['orders']:,} órdenes | gross ${check_base['revenue_gross']:,.2f} | descuento ${check_base['discount_check']:,.2f} | meses {exp_months} | productos {exp_products}")
print(f"Resumen DQ Gold: {len(checks)} checks → {len(checks)-n_fail-n_warn} PASS, {n_warn} WARN, {n_fail} FAIL")
assert n_fail == 0, f"{n_fail} checks de DQ Gold FALLARON"
print("✅ DQ Gold completado y persistido en workspace.default.taco_bueno_dq_results")