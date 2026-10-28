# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Objetivo
# MAGIC %md
# MAGIC # Pipeline Silver — Taco Bueno Pricing
# MAGIC
# MAGIC Reconstruye la capa **Silver** desde la tabla Bronze `workspace.default.taco_bueno_raw_data` (85,273,331 filas, 22 columnas STRING) replicando la lógica validada del notebook `01_Limpieza_Datos`: deduplicación exacta, conversión de tipos, drop de columnas 100% nulas y filtros de outliers POS.
# MAGIC
# MAGIC **Correcciones aplicadas respecto a la ejecución original (Fase 2):**
# MAGIC 1. Tabla nueva `workspace.default.taco_bueno_silver` — el usuario actual es owner (resuelve el bloqueo de SELECT sobre `taco_bueno_clean_data`, propiedad de otro integrante).
# MAGIC 2. **Liquid clustering** por `date` para acelerar análisis temporales de la capa Gold.
# MAGIC 3. **Tabla de cuarentena** `taco_bueno_silver_quarantine` — los registros filtrados NO se eliminan silenciosamente.
# MAGIC 4. **Reconciliación aritmética exacta** raw → silver: separa duplicados de outliers (resuelve la discrepancia 20.19M + 2,364 ≠ 20.19M).
# MAGIC 5. **Resultados de data quality persistidos** en `workspace.default.taco_bueno_dq_results` con patrón delete+insert por capa (reruns seguros, sin duplicados).
# MAGIC
# MAGIC **Grain de la Silver:** una fila = una línea de transacción POS válida y única (tienda × fecha × check × ítem × hora).

# COMMAND ----------

# DBTITLE 1,Crear tabla Silver (CTAS + liquid clustering)
# Configuración: ANSI desactivado → CAST devuelve NULL en valores inválidos (no falla)
spark.conf.set("spark.sql.ansi.enabled", "false")

# CTAS Silver — misma lógica validada de 01_Limpieza_Datos (celda 8), con las correcciones
# documentadas arriba: tabla propia del usuario + liquid clustering por date.
# CREATE OR REPLACE = refresh completo e idempotente (rerun seguro, sin duplicar filas).
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_silver
CLUSTER BY (date)
AS
SELECT DISTINCT
    CAST(Store AS INT) AS store_id,
    TO_DATE(Date, 'M/d/yyyy') AS date,
    CAST(Total AS DOUBLE) AS total,
    Check_Number AS check_number,
    TO_TIMESTAMP(Begin_Time, 'h:mm a') AS begin_time,
    TO_TIMESTAMP(End_Time, 'h:mm a') AS end_time,
    CAST(Total_Discount AS DOUBLE) AS total_discount,
    CAST(Order_Voided AS DOUBLE) AS order_voided,
    CAST(Tax AS DOUBLE) AS tax,
    CAST(Quantity AS INT) AS quantity,
    TRIM(Transaction_Type) AS transaction_type,
    TRIM(Menu_Item) AS menu_item,
    TRIM(Pop) AS pop,
    POS_Code AS pos_code,
    CAST(Menu_Price AS DOUBLE) AS menu_price,
    CAST(Item_Discount AS DOUBLE) AS item_discount,
    CAST(Item_Voided AS DOUBLE) AS item_voided,
    TRIM(Report_Group) AS report_group,
    TRIM(Tender_Type) AS tender_type
FROM workspace.default.taco_bueno_raw_data
WHERE CAST(Quantity AS DOUBLE) <= 50
  AND CAST(Menu_Price AS DOUBLE) <= 50
""")

n_silver = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_silver").collect()[0]['n']
print(f"✅ Silver creada: workspace.default.taco_bueno_silver — {n_silver:,} filas")

# COMMAND ----------

# DBTITLE 1,Tabla de cuarentena (outliers trazables)
# Cuarentena — corrección clave: los registros que fallan los filtros de outliers NO se
# eliminan silenciosamente. Quedan trazables con las mismas transformaciones de tipos
# (donaciones, gift cards, pruebas POS con qty=3,996 o menu_price=$11,111, etc.).
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.taco_bueno_silver_quarantine
AS
SELECT DISTINCT
    CAST(Store AS INT) AS store_id,
    TO_DATE(Date, 'M/d/yyyy') AS date,
    CAST(Total AS DOUBLE) AS total,
    Check_Number AS check_number,
    TO_TIMESTAMP(Begin_Time, 'h:mm a') AS begin_time,
    TO_TIMESTAMP(End_Time, 'h:mm a') AS end_time,
    CAST(Total_Discount AS DOUBLE) AS total_discount,
    CAST(Order_Voided AS DOUBLE) AS order_voided,
    CAST(Tax AS DOUBLE) AS tax,
    CAST(Quantity AS INT) AS quantity,
    TRIM(Transaction_Type) AS transaction_type,
    TRIM(Menu_Item) AS menu_item,
    TRIM(Pop) AS pop,
    POS_Code AS pos_code,
    CAST(Menu_Price AS DOUBLE) AS menu_price,
    CAST(Item_Discount AS DOUBLE) AS item_discount,
    CAST(Item_Voided AS DOUBLE) AS item_voided,
    TRIM(Report_Group) AS report_group,
    TRIM(Tender_Type) AS tender_type
FROM workspace.default.taco_bueno_raw_data
WHERE NOT (CAST(Quantity AS DOUBLE) <= 50 AND CAST(Menu_Price AS DOUBLE) <= 50)
""")

n_quar = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_silver_quarantine").collect()[0]['n']
print(f"✅ Cuarentena creada: workspace.default.taco_bueno_silver_quarantine — {n_quar:,} filas únicas excluidas")

# COMMAND ----------

# DBTITLE 1,Reconciliación Raw → Silver
# Reconciliación aritmética exacta Raw → Silver.
# Resuelve la discrepancia de la Fase 1 (20.19M dups + 2,364 outliers ≠ 20.19M eliminadas):
# los outliers solapan con duplicados — aquí se miden por separado y todo debe cuadrar.
clas = spark.sql("""
SELECT
  COUNT(*) AS raw_total,
  SUM(CASE WHEN CAST(Quantity AS DOUBLE) <= 50 AND CAST(Menu_Price AS DOUBLE) <= 50
           THEN 1 ELSE 0 END) AS raw_validos,
  SUM(CASE WHEN NOT (CAST(Quantity AS DOUBLE) <= 50 AND CAST(Menu_Price AS DOUBLE) <= 50)
           THEN 1 ELSE 0 END) AS raw_outliers
FROM workspace.default.taco_bueno_raw_data
""").collect()[0]

raw_total, raw_validos, raw_outliers = clas['raw_total'], clas['raw_validos'], clas['raw_outliers']
n_silver = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_silver").collect()[0]['n']
n_quar = spark.sql("SELECT COUNT(*) AS n FROM workspace.default.taco_bueno_silver_quarantine").collect()[0]['n']

dups_validos = raw_validos - n_silver

print("=== Reconciliación Raw → Silver ===")
print(f"Raw total:                          {raw_total:,}")
print(f"  ├─ Pasan filtros (con dups):      {raw_validos:,}")
print(f"  │    ├─ Únicos → Silver:          {n_silver:,}  (esperado 65,082,100 = clean original)")
print(f"  │    └─ Duplicados eliminados:     {dups_validos:,}")
print(f"  └─ Fallan filtros (con dups):     {raw_outliers:,}")
print(f"       └─ Únicos → Cuarentena:      {n_quar:,}  (dups dentro de outliers: {raw_outliers - n_quar:,})")

assert raw_validos + raw_outliers == raw_total, f"ERROR: válidos+outliers ({raw_validos + raw_outliers:,}) != raw total"
assert n_silver + dups_validos + raw_outliers == raw_total, "ERROR: la suma no cuadra"
assert n_silver == 65082100, f"ERROR: Silver = {n_silver:,}, esperado 65,082,100 (misma lógica que clean)"
print("✅ Reconciliación exacta: raw_total = únicos Silver + duplicados válidos + outliers con duplicados")

# COMMAND ----------

# DBTITLE 1,Data Quality Checks — Silver
from datetime import datetime

# ---------- Data Quality Checks: Silver ----------
# Cada check define: regla, resultado observado, acción (FAIL = detiene el pipeline,
# WARN = se reporta y continúa). Los resultados se persisten en
# taco_bueno_dq_results con patrón DELETE+INSERT por capa → reruns seguros.

s = spark.sql("""
SELECT COUNT(*) AS total,
       SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_dates,
       SUM(CASE WHEN transaction_type = 'Void' THEN 1 ELSE 0 END) AS void_rows,
       SUM(CASE WHEN transaction_type <> 'Void' THEN 1 ELSE 0 END) AS non_void_rows,
       SUM(CASE WHEN quantity < 0 OR quantity > 50 THEN 1 ELSE 0 END) AS bad_quantity,
       SUM(CASE WHEN quantity = 0 THEN 1 ELSE 0 END) AS zero_quantity,
       SUM(CASE WHEN menu_price > 50 THEN 1 ELSE 0 END) AS bad_price,
       SUM(CASE WHEN transaction_type NOT IN ('Regular Sale','Discount','Void') THEN 1 ELSE 0 END) AS bad_trans_type,
       SUM(CASE WHEN begin_time IS NULL THEN 1 ELSE 0 END) AS null_begin_time,
       MIN(date) AS min_date, MAX(date) AS max_date,
       COUNT(DISTINCT store_id) AS stores
FROM workspace.default.taco_bueno_silver
""").collect()[0]

n_distinct = spark.sql("SELECT COUNT(*) AS n FROM (SELECT DISTINCT * FROM workspace.default.taco_bueno_silver) t").collect()[0]['n']
remaining_dups = s['total'] - n_distinct

now = datetime.utcnow()
def mk(name, rule, observed, expected, ok, action, warn_only=False):
    status = "PASS" if ok else ("WARN" if warn_only else "FAIL")
    return (now, "silver", name, rule, str(observed), expected, status, action)

checks = [
    mk("volumen_silver", "Filas >= 60,000,000 (rango dinámico, no conteo fijo)",
       s['total'], ">= 60,000,000", s['total'] >= 60000000,
       "FAIL del pipeline: volumen inesperado indica cambio en la fuente o en la lógica"),
    mk("duplicados_restantes", "0 filas duplicadas tras SELECT DISTINCT", remaining_dups, "0",
       remaining_dups == 0, "FAIL del pipeline: la deduplicación no funcionó"),
    mk("fechas_nulas", "0 fechas nulas tras TO_DATE", s['null_dates'], "0",
       s['null_dates'] == 0, "FAIL del pipeline: registros con fecha ilegible se pierden del análisis"),
    mk("rango_cantidad", "quantity entre 0 y 50", s['bad_quantity'], "0",
       s['bad_quantity'] == 0, "FAIL del pipeline: outliers atravesaron el filtro"),
    mk("cantidad_cero", "quantity = 0 (líneas sin unidades — se conservan, podrían ser ajustes)",
       s['zero_quantity'], "observar", True,
       "WARN: documentar; no se excluyen (misma decisión que Fase 1)", warn_only=True),
    mk("tope_precio", "menu_price <= 50 (negativos permitidos: promociones)", s['bad_price'], "0",
       s['bad_price'] == 0, "FAIL del pipeline: outliers de precio atravesaron el filtro"),
    mk("dominio_trans_type", "transaction_type ∈ {Regular Sale, Discount, Void}", s['bad_trans_type'], "0",
       s['bad_trans_type'] == 0, "FAIL del pipeline: categoría desconocida requiere revisión"),
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    mk("rango_fechas", "Rango de fechas dinamico (min >= 2025-01-01, max <= hoy)", f"{s['min_date']} a {s['max_date']}",
       f"min >= 2025-01-01, max <= {today_str}",
       str(s['min_date']) >= "2025-01-01" and str(s['max_date']) <= today_str,
       "WARN: verificar si la fuente agregó fechas nuevas (¡podría ser datos frescos!)", warn_only=True),
    mk("tiendas_activas", "Tiendas distintas (>= 100)", s['stores'], ">= 100", s['stores'] >= 100,
       "WARN: cambio en número de tiendas (aperturas/cierres)", warn_only=True),
    mk("begin_time_nulo", "begin_time nulo (afecta análisis por hora, no al resto)",
       s['null_begin_time'], "observar", True,
       "WARN: las tablas hourly excluyen estas filas — documentar cobertura", warn_only=True),
    mk("transacciones_void", "Void conservadas en Silver (excluidas solo en Gold, < 500,000)", s['void_rows'],
       "< 500,000", s['void_rows'] < 500000,
       "WARN: documentar el manejo de Void en el análisis", warn_only=True),
    mk("cuarentena", "Registros excluidos y trazables en quarantine", n_quar,
       "1,000–10,000 (rango histórico)", 1000 <= n_quar <= 10000,
       "WARN: volumen de cuarentena fuera de rango esperado", warn_only=True),
]

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.default.taco_bueno_dq_results (
  checked_at TIMESTAMP, layer STRING, check_name STRING, rule STRING,
  observed STRING, expected STRING, status STRING, action STRING
)""")
spark.sql("DELETE FROM workspace.default.taco_bueno_dq_results WHERE layer = 'silver'")
dq = spark.createDataFrame(checks,
    schema="checked_at timestamp, layer string, check_name string, rule string, observed string, expected string, status string, action string")
dq.write.mode("append").saveAsTable("workspace.default.taco_bueno_dq_results")

n_fail = sum(1 for c in checks if c[6] == "FAIL")
n_warn = sum(1 for c in checks if c[6] == "WARN")
for c in checks:
    icon = "✅" if c[6] == "PASS" else ("⚠️" if c[6] == "WARN" else "❌")
    print(f"{icon} [{c[6]}] {c[2]}: observado={c[4]} (esperado: {c[5]})")
print(f"\nResumen DQ Silver: {len(checks)} checks → {len(checks)-n_fail-n_warn} PASS, {n_warn} WARN, {n_fail} FAIL")
assert n_fail == 0, f"{n_fail} checks de DQ FALLARON — revisar antes de continuar a Gold"
print("✅ DQ Silver completado y persistido en workspace.default.taco_bueno_dq_results")