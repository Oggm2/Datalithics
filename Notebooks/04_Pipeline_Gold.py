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
# MAGIC **Definiciones de métricas (consistentes en todas las tablas):**
# MAGIC * `transactions` = filas no-Void de la Silver (línea de transacción POS)
# MAGIC * `orders` = `COUNT(DISTINCT store_id || '-' || check_number)` — evita colisiones de `check_number` entre tiendas (mejora sobre la EDA Fase 1)
# MAGIC * `revenue` = `SUM(total)`; `avg_order_value` = revenue / orders
# MAGIC * transacción "con descuento" = `total_discount > 0`; `discount_rate_pct` = % de transacciones con descuento
# MAGIC
# MAGIC **Caveats documentados:**
# MAGIC * `total` parece repetir el monto a nivel check por línea → el revenue es comparable con la EDA Fase 1 (misma convención), pero NO es revenue estricto por ítem.
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

# COMMAND ----------

# DBTITLE 1,GOLD 1 — monthly
# GOLD 1 — Métricas mensuales
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_monthly AS
SELECT
    DATE_TRUNC('month', date) AS month,
    COUNT(*) AS transactions,
    COUNT(DISTINCT CONCAT(store_id, '-', check_number)) AS orders,
    ROUND(SUM(total), 2) AS revenue,
    ROUND(SUM(total) / COUNT(DISTINCT CONCAT(store_id, '-', check_number)), 2) AS avg_order_value,
    ROUND(SUM(total_discount), 2) AS discount_amount,
    ROUND(SUM(CASE WHEN total_discount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS discount_rate_pct,
    ROUND(AVG(CASE WHEN total_discount > 0 THEN total END), 2) AS avg_ticket_discounted,
    ROUND(AVG(CASE WHEN total_discount = 0 THEN total END), 2) AS avg_ticket_non_discounted,
    COUNT(DISTINCT store_id) AS active_stores
FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void'
GROUP BY 1
ORDER BY 1
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_monthly").collect()[0]['n']
print(f"✅ taco_bueno_gold_monthly — {n} meses")
spark.sql("SELECT month, transactions, orders, revenue, discount_rate_pct, avg_order_value FROM workspace.default.taco_bueno_gold_monthly ORDER BY month").show(25, truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_product
# MAGIC %md
# MAGIC ## Gold 2 — `taco_bueno_gold_product`
# MAGIC
# MAGIC * **Propósito:** desempeño y descuento por producto (incluye Steak Soft Taco del análisis Fase 1).
# MAGIC * **Grain:** una fila = un `menu_item` (excluye NULL y 'Modifier (NR)' se conserva con su propio caveat).
# MAGIC * **Caveat:** excluye filas con `menu_item` NULL (documentado en DQ); `revenue` hereda la convención de `total` a nivel check.

# COMMAND ----------

# DBTITLE 1,GOLD 2 — product
# GOLD 2 — Desempeño por producto
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_product AS
SELECT
    menu_item,
    MAX(report_group) AS report_group,
    COUNT(*) AS transactions,
    SUM(quantity) AS units_sold,
    ROUND(SUM(total), 2) AS revenue,
    SUM(CASE WHEN total_discount > 0 THEN 1 ELSE 0 END) AS discounted_transactions,
    ROUND(SUM(CASE WHEN total_discount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS discount_rate_pct,
    ROUND(SUM(total_discount), 2) AS discount_amount,
    ROUND(AVG(CASE WHEN total_discount > 0 THEN total END), 2) AS avg_ticket_discounted,
    ROUND(AVG(CASE WHEN total_discount = 0 THEN total END), 2) AS avg_ticket_non_discounted,
    ROUND(SUM(total_discount) / NULLIF(SUM(quantity), 0), 4) AS discount_per_unit
FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void'
  AND menu_item IS NOT NULL
GROUP BY menu_item
ORDER BY revenue DESC
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_product").collect()[0]['n']
print(f"✅ taco_bueno_gold_product — {n} productos")
spark.sql("""SELECT menu_item, transactions, units_sold, revenue, discount_rate_pct, discount_amount
FROM workspace.default.taco_bueno_gold_product
WHERE transactions >= 10000 ORDER BY discount_rate_pct DESC LIMIT 10""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_channel
# MAGIC %md
# MAGIC ## Gold 3 — `taco_bueno_gold_channel`
# MAGIC
# MAGIC * **Propósito:** volumen y descuento por canal de venta (`pop`): Drive Thru, To Go, Online: DoorDash, Mobile, etc.
# MAGIC * **Grain:** una fila = un canal (`pop`). Incluye el canal `3` (atributo POS adicional válido, decisión de Fase 1).

# COMMAND ----------

# DBTITLE 1,GOLD 3 — channel
# GOLD 3 — Desempeño por canal
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_channel AS
SELECT
    pop AS channel,
    COUNT(*) AS transactions,
    COUNT(DISTINCT CONCAT(store_id, '-', check_number)) AS orders,
    ROUND(SUM(total), 2) AS revenue,
    ROUND(SUM(total) / COUNT(DISTINCT CONCAT(store_id, '-', check_number)), 2) AS avg_order_value,
    ROUND(SUM(total_discount), 2) AS discount_amount,
    ROUND(SUM(CASE WHEN total_discount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS discount_rate_pct,
    ROUND(AVG(CASE WHEN total_discount > 0 THEN total END), 2) AS avg_ticket_discounted,
    ROUND(AVG(CASE WHEN total_discount = 0 THEN total END), 2) AS avg_ticket_non_discounted,
    COUNT(DISTINCT store_id) AS active_stores
FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void'
GROUP BY 1
ORDER BY transactions DESC
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_channel").collect()[0]['n']
print(f"✅ taco_bueno_gold_channel — {n} canales")
spark.sql("SELECT * FROM workspace.default.taco_bueno_gold_channel").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_hourly
# MAGIC %md
# MAGIC ## Gold 4 — `taco_bueno_gold_hourly`
# MAGIC
# MAGIC * **Propósito:** ventas por hora del día — base para propuestas de promociones de tarde (Fase 1).
# MAGIC * **Grain:** una fila = una hora del día (0–23), agregando todos los días y tiendas.
# MAGIC * **Caveat:** la hora viene de `begin_time` (solo hora, sin fecha real del evento); 0 filas con hora nula según DQ de la Silver.

# COMMAND ----------

# DBTITLE 1,GOLD 4 — hourly
# GOLD 4 — Ventas por hora del día
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_hourly AS
SELECT
    HOUR(begin_time) AS hour_of_day,
    COUNT(*) AS transactions,
    COUNT(DISTINCT CONCAT(store_id, '-', check_number)) AS orders,
    ROUND(SUM(total), 2) AS revenue,
    ROUND(AVG(total), 2) AS avg_ticket,
    ROUND(SUM(total_discount), 2) AS discount_amount,
    ROUND(SUM(CASE WHEN total_discount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS discount_rate_pct
FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void'
  AND begin_time IS NOT NULL
GROUP BY 1
ORDER BY 1
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_hourly").collect()[0]['n']
print(f"✅ taco_bueno_gold_hourly — {n} horas con ventas")
spark.sql("SELECT * FROM workspace.default.taco_bueno_gold_hourly ORDER BY revenue DESC LIMIT 8").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_dayofweek
# MAGIC %md
# MAGIC ## Gold 5 — `taco_bueno_gold_dayofweek`
# MAGIC
# MAGIC * **Propósito:** desempeño por día de la semana y fin de semana vs día laboral — base para propuestas de bundles de fin de semana (Fase 1).
# MAGIC * **Grain:** una fila = un día de la semana (1=domingo … 7=sábado, convención Spark).
# MAGIC * **Métrica clave:** `avg_orders_per_store_day` = órdenes promedio por tienda por día (misma métrica de tráfico de la EDA Fase 1).

# COMMAND ----------

# DBTITLE 1,GOLD 5 — dayofweek
# GOLD 5 — Desempeño por día de la semana (con órdenes por tienda por día)
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_dayofweek AS
WITH store_daily AS (
    SELECT
        date,
        DAYOFWEEK(date) AS dow,
        DATE_FORMAT(date, 'EEEE') AS day_name,
        store_id,
        COUNT(*) AS transactions,
        COUNT(DISTINCT check_number) AS orders,
        SUM(total) AS revenue,
        SUM(total_discount) AS discount_amount,
        SUM(CASE WHEN total_discount > 0 THEN 1 ELSE 0 END) AS discounted_transactions
    FROM workspace.default.taco_bueno_silver
    WHERE transaction_type != 'Void'
    GROUP BY 1, 2, 3, 4
)
SELECT
    dow AS day_of_week_num,
    day_name,
    dow IN (1, 7) AS is_weekend,
    COUNT(*) AS store_days,
    SUM(transactions) AS transactions,
    SUM(orders) AS orders,
    ROUND(SUM(orders) * 1.0 / COUNT(*), 1) AS avg_orders_per_store_day,
    ROUND(SUM(revenue), 2) AS revenue,
    ROUND(SUM(discount_amount), 2) AS discount_amount,
    ROUND(SUM(discounted_transactions) * 100.0 / SUM(transactions), 2) AS discount_rate_pct
FROM store_daily
GROUP BY dow, day_name
ORDER BY dow
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_dayofweek").collect()[0]['n']
print(f"✅ taco_bueno_gold_dayofweek — {n} días de la semana")
spark.sql("SELECT * FROM workspace.default.taco_bueno_gold_dayofweek").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — gold_promo_effectiveness
# MAGIC %md
# MAGIC ## Gold 6 — `taco_bueno_gold_promo_effectiveness`
# MAGIC
# MAGIC * **Propósito:** comparar transacciones con y sin descuento por mes (tasa, ticket, revenue) — la tabla de efectividad de promociones.
# MAGIC * **Grain:** una fila = mes × segmento (`Con descuento` / `Sin descuento`).
# MAGIC * **Caveat explícito:** comparación **descriptiva**. Sin grupo de control no puede afirmarse causalidad (el descuento podría correlacionarse con promociones estacionales). El costo del descuento se mide con `discount_amount`.

# COMMAND ----------

# DBTITLE 1,GOLD 6 — promo_effectiveness
# GOLD 6 — Efectividad de promociones: con vs sin descuento, por mes
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_gold_promo_effectiveness AS
SELECT
    DATE_TRUNC('month', date) AS month,
    CASE WHEN total_discount > 0 THEN 'Con descuento' ELSE 'Sin descuento' END AS segment,
    COUNT(*) AS transactions,
    COUNT(DISTINCT CONCAT(store_id, '-', check_number)) AS orders,
    SUM(quantity) AS units,
    ROUND(SUM(total), 2) AS revenue,
    ROUND(SUM(total) / COUNT(DISTINCT CONCAT(store_id, '-', check_number)), 2) AS avg_order_value,
    ROUND(AVG(total), 2) AS avg_ticket,
    ROUND(SUM(total_discount), 2) AS discount_amount
FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void'
GROUP BY 1, 2
ORDER BY 1, 2
""")

n = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_gold_promo_effectiveness").collect()[0]['n']
print(f"✅ taco_bueno_gold_promo_effectiveness — {n} filas (mes × segmento)")
spark.sql("""SELECT month, segment, transactions, revenue, avg_ticket, discount_amount
FROM workspace.default.taco_bueno_gold_promo_effectiveness
WHERE month >= '2026-07-01' ORDER BY month, segment""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Doc — DQ Gold
# MAGIC %md
# MAGIC ## DQ Checks — capa Gold
# MAGIC
# MAGIC Reconciliación cruzada: cada tabla Gold debe reproducir los totales de la Silver no-Void (transacciones exactas; revenue dentro de una tolerancia de redondeo de $5). Excepción documentada: `gold_product` excluye filas con `menu_item` NULL y se reconcilia contra esa base. Resultados persistidos en `taco_bueno_dq_results` (layer = `gold`).

# COMMAND ----------

# DBTITLE 1,DQ Checks — Gold (reconciliación cruzada)
from datetime import datetime, timezone

# ---------- Data Quality Checks: Gold (reconciliación cruzada con Silver) ----------
TOL = 5.0  # tolerancia por redondeo a 2 decimales por fila agregada

base = spark.sql("""
SELECT COUNT(*) AS n, ROUND(SUM(total), 2) AS revenue
FROM workspace.default.taco_bueno_silver WHERE transaction_type != 'Void'
""").collect()[0]
base_prod = spark.sql("""
SELECT COUNT(*) AS n, ROUND(SUM(total), 2) AS revenue
FROM workspace.default.taco_bueno_silver
WHERE transaction_type != 'Void' AND menu_item IS NOT NULL
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

def agg(table, sum_col="revenue", cnt_col="transactions"):
    r = spark.sql(f"""SELECT COUNT(*) AS rows, SUM({cnt_col}) AS trans, ROUND(SUM({sum_col}), 2) AS rev
                   FROM workspace.default.{table}""").collect()[0]
    return r['rows'], r['trans'], r['rev']

now = datetime.now(timezone.utc)
def mk(name, rule, observed, expected, ok, action, warn_only=False):
    status = "PASS" if ok else ("WARN" if warn_only else "FAIL")
    return (now, "gold", name, rule, str(observed), expected, status, action)

checks = []

# --- gold_monthly ---
rows, trans, rev = agg("taco_bueno_gold_monthly")
checks.append(mk("gold_monthly_filas", "Filas == meses distintos en Silver", rows, exp_months,
                  rows == exp_months, "FAIL: meses faltantes o duplicados"))
checks.append(mk("gold_monthly_revenue", "SUM(revenue) ≈ Silver no-Void (tol $5)", f"${rev:,.2f}",
                  f"${base['revenue']:,.2f}", abs(rev - base['revenue']) <= TOL,
                  "FAIL: la agregación mensual no reconcilia con la Silver"))
checks.append(mk("gold_monthly_trans", "SUM(transactions) == Silver no-Void", trans, base['n'],
                  trans == base['n'], "FAIL: transacciones perdidas o duplicadas"))

# --- gold_channel ---
rows, trans, rev = agg("taco_bueno_gold_channel")
checks.append(mk("gold_channel_filas", "Filas == canales (pop) distintos", rows, exp_channels,
                  rows == exp_channels, "FAIL: canal faltante"))
checks.append(mk("gold_channel_revenue", "SUM(revenue) ≈ Silver no-Void (tol $5)", f"${rev:,.2f}",
                  f"${base['revenue']:,.2f}", abs(rev - base['revenue']) <= TOL,
                  "FAIL: la agregación por canal no reconcilia"))

# --- gold_hourly ---
rows, trans, rev = agg("taco_bueno_gold_hourly")
checks.append(mk("gold_hourly_filas", "Filas == horas con ventas (esperado 24)", rows, "24",
                  rows == 24, "WARN: alguna hora sin ventas — verificar cobertura", warn_only=True))
checks.append(mk("gold_hourly_revenue", "SUM(revenue) ≈ Silver no-Void (tol $5)", f"${rev:,.2f}",
                  f"${base['revenue']:,.2f}", abs(rev - base['revenue']) <= TOL,
                  "FAIL: la agregación por hora no reconcilia"))

# --- gold_dayofweek ---
rows, trans, rev = agg("taco_bueno_gold_dayofweek")
checks.append(mk("gold_dayofweek_filas", "Filas == 7 días de la semana", rows, "7",
                  rows == 7, "FAIL: día faltante"))
checks.append(mk("gold_dayofweek_revenue", "SUM(revenue) ≈ Silver no-Void (tol $5)", f"${rev:,.2f}",
                  f"${base['revenue']:,.2f}", abs(rev - base['revenue']) <= TOL,
                  "FAIL: la agregación por día no reconcilia"))

# --- gold_product (base distinta: sin menu_item NULL, documentado) ---
rows, trans, rev = agg("taco_bueno_gold_product")
checks.append(mk("gold_product_filas", "Filas == menu_items distintos no-NULL", rows, exp_products,
                  rows == exp_products, "FAIL: producto faltante"))
checks.append(mk("gold_product_revenue", "SUM(revenue) ≈ Silver no-Void sin menu_item NULL (tol $5)",
                  f"${rev:,.2f}", f"${base_prod['revenue']:,.2f}",
                  abs(rev - base_prod['revenue']) <= TOL,
                  "FAIL: la agregación por producto no reconcilia con su base"))
checks.append(mk("gold_product_cobertura", "Filas NULL de menu_item excluidas (documentado)",
                  f"{base['n'] - base_prod['n']:,} filas", "documentado", True,
                  "WARN: documentar la exclusión de menú NULL en el análisis", warn_only=True))

# --- gold_promo_effectiveness ---
rows, trans, rev = agg("taco_bueno_gold_promo_effectiveness")
checks.append(mk("gold_promo_filas", "Filas == 2 segmentos × meses", rows, 2 * exp_months,
                  rows == 2 * exp_months, "FAIL: segmento o mes faltante"))
checks.append(mk("gold_promo_trans", "SUM(transactions) == Silver no-Void (partición completa)",
                  trans, base['n'], trans == base['n'],
                  "FAIL: la partición con/sin descuento pierde o duplica transacciones"))
checks.append(mk("gold_promo_revenue", "SUM(revenue) ≈ Silver no-Void (tol $5)", f"${rev:,.2f}",
                  f"${base['revenue']:,.2f}", abs(rev - base['revenue']) <= TOL,
                  "FAIL: la partición con/sin descuento no reconcilia"))

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
print(f"\nBase no-Void: {base['n']:,} transacciones, ${base['revenue']:,.2f} revenue | meses: {exp_months} | productos: {exp_products}")
print(f"Resumen DQ Gold: {len(checks)} checks → {len(checks)-n_fail-n_warn} PASS, {n_warn} WARN, {n_fail} FAIL")
assert n_fail == 0, f"{n_fail} checks de DQ Gold FALLARON"
print("✅ DQ Gold completado y persistido en workspace.default.taco_bueno_dq_results")