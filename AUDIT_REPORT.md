# Auditoría del Pipeline Taco Bueno Pricing — Fase 2
## Reporte de Auditoría Técnica

**Fecha de auditoría:** 28 de octubre, 2026  
**Auditor:** Senior Databricks Data Engineer (asistido por IA)  
**Alcance:** 6 notebooks, 11 tablas, 3 Jobs, 27 DQ checks, 8 hipótesis

---

## 1. Resumen Ejecutivo

El pipeline Taco Bueno Pricing está **funcionalmente correcto y listo para presentación**, con excepción de un hallazgo de alto impacto en el análisis de hipótesis (H4) y varios issues de robustez para producción.

**Hallazgos clave:**
- ✅ **Reconciliación de datos exacta:** 85,273,331 raw = 65,082,100 Silver + 20,188,867 duplicados + 1,586 cuarentena + 778 dups en outliers. Cuadra perfectamente.
- ✅ **Revenue y transacciones reconcilian exactamente** entre Silver y todas las tablas Gold (excepto gold_product que excluye NULL menu_item, documentado).
- ✅ **27/27 DQ checks pasan** — verificado con resultados reales de ejecución.
- ✅ **Liquid clustering** confirmado: CLUSTER BY (date) activo en Silver.
- ✅ **3 Jobs existen y están activos** con schedules correctos.
- ❌ **H4 (Happy hour 3-6 PM) no está respaldada por los datos:** el revenue realmente sube de 3 PM a 6 PM; el valle es de 1-3 PM.
- ⚠️ **DQ checks con conteos fijos** (65,082,100; 119 tiendas; fechas hardcodeadas) fallarán cuando lleguen datos nuevos.
- ⚠️ **No hay política de reintentos** en los Jobs, aunque el README lo afirma.
- ⚠️ **Conteo de órdenes inflado en 47,351** (check_numbers reutilizados entre meses).

**Veredicto:** El pipeline es presentable con las correcciones documentadas en la Sección 10. No se requieren reconstrucciones.

---

## 2. Componentes Inspeccionados y Limitaciones de Acceso

### Componentes inspeccionados (verificados directamente)

| Componente | Método | Verificado |
|---|---|---|
| 01_Limpieza_Datos.ipynb | readAssetById (25 celdas) | ✅ Código fuente leído |
| 02_EDA.ipynb | readAssetById (25 celdas) | ✅ Código + resultados leídos |
| 03_Pipeline_Silver.py | readAssetById (5 celdas) | ✅ Código + resultados leídos |
| 04_Pipeline_Gold.py | readAssetById (14 celdas) | ✅ Código + resultados leídos |
| 05_Pipeline_Job.ipynb | readAssetById (6 celdas) | ✅ Código + resultados leídos |
| 06_Analisis_Hipotesis.ipynb | readAssetById (11 celdas) | ✅ Código + resultados leídos |
| 11 tablas SQL | SELECT COUNT(*) / DESCRIBE | ✅ Conteos y esquemas verificados |
| 3 Jobs | readAssetById (job) | ✅ Configuración completa leída |
| 27 DQ checks | SELECT FROM dq_results | ✅ Resultados reales verificados |
| Reconciliación raw→silver→gold | spark.sql en vivo | ✅ Ejecutado en compute |

### Limitaciones de acceso

- No se ejecutaron los notebooks durante la auditoría (solo lectura de resultados previos).
- No se modificaron tablas — todas las queries fueron SELECT.
- No se verificó el contenido del UC Volume taco_bueno_files (solo se verificó que la tabla raw existe).
- No se probaron reruns del pipeline (no se ejecutó 05_Pipeline_Job).

---

## 3. Arquitectura Verificada y Conteos Reales de Tablas

### Conteos verificados (ejecutados en compute)

| Tabla | Filas documentadas | Filas reales | Match |
|---|---|---|---|
| taco_bueno_raw_data | 85,273,331 | 85,273,331 | ✅ |
| taco_bueno_silver | 65,082,100 | 65,082,100 | ✅ |
| taco_bueno_silver_quarantine | 1,586 | 1,586 | ✅ |
| taco_bueno_gold_monthly | 21 | 21 | ✅ |
| taco_bueno_gold_product | 643 | 643 | ✅ |
| taco_bueno_gold_channel | 12 | 12 | ✅ |
| taco_bueno_gold_hourly | 24 | 24 | ✅ |
| taco_bueno_gold_dayofweek | 7 | 7 | ✅ |
| taco_bueno_gold_promo_effectiveness | — | 42 | ✅ (2 segmentos × 21 meses) |
| taco_bueno_dq_results | 27 | 27 | ✅ |
| taco_bueno_kpi_snapshot | — | 9 | ✅ |

---

## 4. Resultados de Reconciliación de Datos

### Reconciliación Raw → Silver (verificada en compute)

```
Raw total:                          85,273,331
  ├─ Pasan filtros (con dups):      85,270,967
  │    ├─ Únicos → Silver:          65,082,100  ✅
  │    └─ Duplicados eliminados:    20,188,867
  └─ Fallan filtros (con dups):      2,364
       ├─ Únicos → Cuarentena:       1,586  ✅
       └─ Dups dentro de outliers:      778

Reconstruido: 65,082,100 + 20,188,867 + 1,586 + 778 = 85,273,331 ✅ EXACTO
```

### Reconciliación Silver → Gold (verificada en compute)

| Métrica | Silver non-Void | Gold SUM | Diff | Status |
|---|---|---|---|---|
| Transactions | 64,862,789 | 64,862,789 | 0 | ✅ |
| Revenue | $1,150,140,925.30 | $1,150,140,925.30 | $0.00 | ✅ |
| Discount amount | $24,140,633.67 | $24,140,633.67 | $0.00 | ✅ |
| Orders (global distinct) | 13,728,036 | 13,775,387 (sum monthly) | 47,351 | ⚠️ |

### Issues de reconciliación

**Issue #1 — Conteo de órdenes inflado (Severidad: Media)**  
Gold SUM(orders) mensual = 13,775,387, pero el conteo global de COUNT(DISTINCT store_id || '-' || check_number) = 13,728,036. La diferencia de 47,351 se debe a 47,348 check_numbers reutilizados entre meses diferentes. No es un bug — es inherente a sumar conteos distinct por partición temporal. Pero el README reporta 13,775,387 como "total orders" sin esta salvedad.

**Issue #2 — gold_product excluye 2,943,059 filas con menu_item NULL (Severidad: Baja)**  
Gold product tiene 61,919,730 transacciones vs 64,862,789 en Silver non-Void. La diferencia está documentada en el DQ check gold_product_cobertura (PASS, status WARN). El revenue de gold_product ($1,094M) es $56.3M menor al total.

**Issue #3 — Valor negativo en promo_effectiveness (Severidad: Baja)**  
Una fila en gold_promo_effectiveness tiene discount_amount = -$11.92 en el segmento "Sin descuento" (Feb 2025). Probablemente un artifact de valores residuales negativos de total_discount.

---

## 5. Auditoría de Data Quality (27 Checks)

### Silver (12 checks)

| # | Check name | Regla | Expected | Observed | Status | Severidad | Apropiado? |
|---|---|---|---|---|---|---|---|
| 1 | volumen_silver | Filas == 65,082,100 | 65,082,100 | 65,082,100 | PASS | FAIL | ⚠️ Conteo fijo |
| 2 | duplicados_restantes | 0 dups tras DISTINCT | 0 | 0 | PASS | FAIL | ✅ |
| 3 | fechas_nulas | 0 fechas nulas | 0 | 0 | PASS | FAIL | ✅ |
| 4 | rango_cantidad | quantity 0-50 | 0 | 0 | PASS | FAIL | ✅ |
| 5 | cantidad_cero | quantity=0 (observar) | observar | 4,701,615 | PASS | WARN | ✅ |
| 6 | tope_precio | menu_price ≤ 50 | 0 | 0 | PASS | FAIL | ✅ |
| 7 | dominio_trans_type | tipos válidos | 0 | 0 | PASS | FAIL | ✅ |
| 8 | rango_fechas | 2025-01-22 a 2026-09-27 | hardcoded | exacto | PASS | WARN | ⚠️ Fechas hardcodeadas |
| 9 | tiendas_activas | 119 tiendas | 119 | 119 | PASS | WARN | ⚠️ Conteo fijo |
| 10 | transacciones_void | Voids conservados | 219,311 | 219,311 | PASS | WARN | ⚠️ Conteo fijo |
| 11 | begin_time_nulo | begin_time nulo | observar | 0 | PASS | WARN | ✅ |
| 12 | cuarentena | Outliers trazables | 1,000-10,000 | 1,586 | PASS | WARN | ✅ Rango flexible |

### Gold (15 checks)

| # | Check name | Regla | Expected | Observed | Status |
|---|---|---|---|---|---|
| 13 | gold_monthly_filas | 21 meses | 21 | 21 | PASS |
| 14 | gold_monthly_revenue | SUM(rev) ≈ Silver (tol $5) | $1,150,140,925.30 | $1,150,140,925.30 | PASS |
| 15 | gold_monthly_trans | SUM(tx) == Silver | 64,862,789 | 64,862,789 | PASS |
| 16 | gold_channel_filas | 12 canales | 12 | 12 | PASS |
| 17 | gold_channel_revenue | SUM(rev) ≈ Silver | $1,150,140,925.30 | $1,150,140,925.30 | PASS |
| 18 | gold_product_filas | 643 productos | 643 | 643 | PASS |
| 19 | gold_product_revenue | SUM(rev) sin NULL menu | $1,093,863,724.15 | $1,093,863,724.15 | PASS |
| 20 | gold_product_cobertura | NULL menu_item excluidas | documentado | 2,943,059 | PASS |
| 21 | gold_hourly_filas | 24 horas | 24 | 24 | PASS |
| 22 | gold_hourly_revenue | SUM(rev) ≈ Silver | $1,150,140,925.30 | $1,150,140,925.30 | PASS |
| 23 | gold_dayofweek_filas | 7 días | 7 | 7 | PASS |
| 24 | gold_dayofweek_revenue | SUM(rev) ≈ Silver | $1,150,140,925.30 | $1,150,140,925.30 | PASS |
| 25 | gold_promo_filas | 2 × 21 = 42 | 42 | 42 | PASS |
| 26 | gold_promo_revenue | SUM(rev) ≈ Silver | $1,150,140,925.30 | $1,150,140,925.30 | PASS |
| 27 | gold_promo_trans | SUM(tx) == Silver | 64,862,789 | 64,862,789 | PASS |

**Todos los 27 checks pasan ✅**

### Issues de DQ

**Issue #4 — DQ checks con conteos y fechas fijas (Severidad: Alta)**  
Los checks volumen_silver (65,082,100), rango_fechas (hardcoded), tiendas_activas (119), transacciones_void (219,311) y gold_monthly_filas (21) usan valores hardcodeados que fallarán cuando lleguen datos nuevos. Deberían usar rangos dinámicos o comparaciones relativas.

**Issue #5 — DQ checks de revenue usan tolerancia fija de $5 (Severidad: Baja)**  
La tolerancia de $5 puede ser apropiada hoy, pero a medida que crezca el dataset, diferencias de redondeo podrían excederla. Recomendación: tolerancia porcentual (0.001%).

**Issue #6 — Gold se publica ANTES de verificar DQ (Severidad: Media)**  
El notebook 05 hace CREATE OR REPLACE TABLE en Gold (celda 04) ANTES de verificar DQ (celda 05). Si un check Gold falla, las tablas Gold ya fueron reemplazadas. El assert detiene el Job pero el daño ya está hecho.

---

## 6. Auditoría de Definiciones de Métricas

| Métrica | Definición (del código) | Consistente | Observaciones |
|---|---|---|---|
| revenue | SUM(total) non-Void | ✅ Exacto | total puede ser negativo (54 filas) |
| transactions | COUNT(*) non-Void | ✅ Exacto | Líneas POS, no órdenes |
| orders | COUNT(DISTINCT store_id \|\| '-' \|\| check_number) | ⚠️ Sum mensual inflada | 47,351 overcount |
| discount_amount | SUM(total_discount) non-Void | ✅ Exacto | |
| discount_rate_pct | % con total_discount > 0 | ✅ | |
| avg_order_value | revenue / orders | ✅ | Usa order count inflado |
| avg_ticket_discounted | AVG(total) WHERE total_discount > 0 | ✅ | |
| orders_per_store_day | orders / store_days | ✅ | |

---

## 7. Auditoría de Jobs y Orquestación

### Jobs verificados (configuración real)

| Job | ID | Trigger | Timezone | Status | Cadencia |
|---|---|---|---|---|---|
| Semanal | 613936090449600 | Periodic, 1 WEEK | — | UNPAUSED | semanal |
| Quincenal | 354096890254923 | Periodic, 2 WEEKS | — | UNPAUSED | quincenal |
| Mensual | 1119533549743462 | Cron 0 0 9 1 * ? * | America/Mexico_City | UNPAUSED | mensual |

Todos los Jobs: 1 tarea (notebook 05_Pipeline_Job), queue enabled, sin timeout, sin email/webhook, sin retry policy.

Runs recientes: todos SUCCESS.

### Issues de orquestación

**Issue #9 — No hay política de reintentos (Severidad: Alta)**  
El README afirma "Jobs are configured to retry on transient failures." La configuración real NO tiene max_retries ni retry_on_timeout.

**Issue #10 — Tres Jobs redundantes (Severidad: Media)**  
Los 3 Jobs ejecutan el mismo pipeline. Si todos están UNPAUSED, corre ~5 veces/mes. Recomendación: pausar 2 de 3 para producción.

**Issue #11 — Sin notificaciones de fallo (Severidad: Media)**  
Ningún Job tiene email/webhook notifications.

**Issue #12 — Sin timeout (Severidad: Baja)**  
timeout_seconds: 0 significa sin límite. Recomendación: 3600 segundos.

---

## 8. Evaluación de Technical Best Practices

| Práctica | Implementación real | Veredicto |
|---|---|---|
| Liquid clustering | CLUSTER BY (date) en Silver | ✅ Confirmado via TBLPROPERTIES |
| CTAS idempotente | CREATE OR REPLACE TABLE | ✅ |
| DQ expectations | 27 checks custom + asserts | ✅ Con issues de conteos fijos |
| Cuarentena | 1,586 filas trazables | ✅ |
| Streaming | No implementado (batch) | ✅ Justificado |
| Materialized views | No usadas (CTAS) | ✅ Justificado |
| ANSI mode off | spark.sql.ansi.enabled = false | ✅ |
| Full recompute | 85M rows, ~5 min serverless | ⚠️ Aceptable hoy |
| DELETE+INSERT DQ | Idempotente por capa | ✅ |
| KPI snapshot append-only | INSERT only | ✅ |

---

## 9. Limitaciones del Análisis de Negocio

### H1 — Drive Thru no necesita descuentos
**Veredicto: ✅ Confirmado, con matiz**  
4.23% discount rate, 17.99M transacciones, $304.5M revenue verificados. Pero DoorDash tiene 0% — el claim "lowest of any major channel" debería ser "lowest among promoted channels".

### H2 — Steak Soft Taco canibalizó su precio
**Veredicto: ✅ Confirmado exactamente**  
73.29% discount rate, $429,621.92 verificados contra gold_product.

### H3 — Recortar descuentos no causó caída de tráfico
**Veredicto: ✅ Confirmado, con caveats**  
Descuento cayó 22.27% → 6-7%, tráfico se mantuvo. Caveat 1: enero 2025 tiene solo 10 días (partial month). Caveat 2: esto es correlación temporal, no causalidad — no se controlaron variables externas.

### H4 — Oportunidad de happy hour 3-6 PM
**Veredicto: ❌ NO RESPALDADA por los datos**  
Datos reales (gold_hourly):
- 12:00 — $109.1M (pico lunch)
- 13:00 — $77.8M (bajando)
- 14:00 — $65.9M (VALLE REAL)
- 15:00 — $77.3M (recuperando)
- 16:00 — $117.2M (subiendo)
- 17:00 — $139.0M (PICO CENA)
- 18:00 — $121.1M

El revenue SUBE de 3 PM a 6 PM. El valle real es 1-3 PM (14:00 es el mínimo). La hipótesis debería corregirse a "happy hour 1-3 PM".

### H5 — App móvil con descuento desproporcionado
**Veredicto: ✅ Confirmado exactamente**  
7,486 transacciones, 50.91% discount, $178K vs DoorDash 2M transacciones, 0%, $55.4M.

### H6 — Promos de fin de semana
**Veredicto: ⚠️ Evaluar (apropiadamente cauteloso)**  
Domingo 216 vs miércoles 238 órdenes/tienda/día. Veredicto de A/B test correcto.

### H7 — Descuentos en órdenes pequeñas
**Veredicto: ⚠️ Evaluar (apropiadamente cauteloso)**  
5.47M órdenes <$10 (39.8%), 564K con descuento. Caveat de A/B correcto.

### H8 — Sides y condimentos son giveaways
**Veredicto: ⚠️ Evaluar (apropiadamente cauteloso)**  
$5.26M en descuentos verificado. Caveat de canasta incremental correcto.

**Issue #14 — H3 implica causalidad sin A/B (Severidad: Media)**  
"No causó caída" es una inferencia causal. Debería ser: "no se observó caída asociada temporalmente con el recorte."

---

## 10. Issues Priorizados

| # | Severidad | Issue | Componente | Acción |
|---|---|---|---|---|
| 1 | ALTA | H4: happy hour 3-6 PM no respaldada | 06_Analisis_Hipotesis, README | Corregir a 1-3 PM |
| 2 | ALTA | DQ checks con conteos/fechas fijas | 03/04 notebooks | Cambiar a rangos dinámicos |
| 3 | ALTA | No hay política de reintentos | 3 Jobs, README | Agregar max_retries o corregir README |
| 4 | MEDIA | Gold se publica antes de DQ | 05_Pipeline_Job | Pre-check antes de CTAS |
| 5 | MEDIA | 3 Jobs redundantes | 3 Jobs | Pausar 2 de 3 |
| 6 | MEDIA | Sin notificaciones de fallo | 3 Jobs | Configurar email on_failure |
| 7 | MEDIA | Conteo de órdenes inflado 47,351 | README, SUMMARY | Aclarar que es suma mensual |
| 8 | MEDIA | H3 implica causalidad sin A/B | 06_Analisis_Hipotesis | Agregar caveat |
| 9 | BAJA | H1: "lowest discount rate" inexacto | 06_Analisis_Hipotesis | "lowest among promoted" |
| 10 | BAJA | H3: enero 2025 partial month | 06_Analisis_Hipotesis | Excluir o documentar |
| 11 | BAJA | Valor -$11.92 en promo_effectiveness | 04_Pipeline_Gold | Investigar origen |
| 12 | BAJA | KPI snapshot con cadencias NULL | kpi_snapshot | Backfill o dejar histórico |
| 13 | BAJA | Sin timeout en Jobs | 3 Jobs | Agregar 3600s |
| 14 | BAJA | Tolerancia DQ fija $5 | 03/04 notebooks | Cambiar a porcentual |

---

## 11. Fixes Recomendados

### Fix 1 (ALTA): Corregir H4 — happy hour
Archivo: 06_Analisis_Hipotesis.ipynb, celda H4
Cambiar el análisis de 3-6 PM a 1-3 PM. Actualizar README y PROJECT_SUMMARY.

### Fix 2 (ALTA): DQ checks con conteos dinámicos
Archivo: 03_Pipeline_Silver.py, celda DQ
- volumen_silver: cambiar expected fijo a ">= 60,000,000"
- rango_fechas: cambiar a "MIN(date) >= '2025-01-01' AND MAX(date) <= current_date()"
- tiendas_activas: cambiar a ">= 100"
- gold_monthly_filas: calcular dinámicamente desde Silver

### Fix 3 (ALTA): Política de reintentos
Componente: Los 3 Jobs
Agregar: max_retries=1, retry_on_timeout=true, min_retry_interval_millis=60000
O corregir el README para remover la afirmación.

### Fix 4 (MEDIA): Pre-verificación de Gold antes de CTAS
Archivo: 04_Pipeline_Gold.py
Agregar sanity check de Silver antes de cada CREATE OR REPLACE TABLE Gold.

### Fix 5 (MEDIA): Notificaciones de fallo
Componente: Los 3 Jobs
Agregar email_notifications.on_failure con email del equipo.

### Fix 7 (MEDIA): Aclarar conteo de órdenes
Archivo: README.md, PROJECT_SUMMARY.md
Cambiar "13,775,387 orders" a "13,775,387 monthly order-months (global distinct: 13,728,036)".

### Fix 8 (MEDIA): Caveat de causalidad en H3
Archivo: 06_Analisis_Hipotesis.ipynb, celda H3
Agregar: "Esta observación es temporal, no causal. Recomendamos A/B test."

---

## 12. Evidencia y Screenshots para la Presentación

### Screenshots obligatorios

1. Arquitectura Medallion — Diagrama del README
2. Reconciliación raw→silver — Output del notebook 03
3. DQ results — SELECT FROM taco_bueno_dq_results (27 rows, all PASS)
4. KPI snapshot — SELECT FROM taco_bueno_kpi_snapshot ORDER BY run_ts DESC
5. Jobs UI — 3 Jobs en Databricks
6. Job run history — Runs SUCCESS
7. Liquid clustering — SHOW TBLPROPERTIES taco_bueno_silver
8. EDA charts — 3 gráficas del notebook 02
9. Hypothesis table — Tabla H1-H8 del README
10. GitHub repo — github.com/Oggm2/Datalithics

### Queries de verificación en vivo (para Q&A)

```sql
SELECT layer, COUNT(*) AS checks, SUM(CASE WHEN status != 'PASS' THEN 1 ELSE 0 END) AS fails
FROM workspace.default.taco_bueno_dq_results GROUP BY layer;

SELECT SUM(revenue) FROM workspace.default.taco_bueno_gold_monthly;
SELECT ROUND(SUM(total), 2) FROM workspace.default.taco_bueno_silver WHERE transaction_type != 'Void';

SHOW TBLPROPERTIES workspace.default.taco_bueno_silver;
```

---

*Auditoría completada sin modificar tablas, ejecutar pipelines, ni cambiar configuración de Jobs. Todas las verificaciones fueron queries SELECT de solo lectura.*
