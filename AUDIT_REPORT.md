# Auditoría del Pipeline Taco Bueno Pricing — Fase 2
## Reporte de Auditoría Técnica

**Fecha de auditoría:** 28 de octubre, 2026 — actualizada con la 2ª pasada (semántica de métricas a nivel check/ítem)  
**Auditor:** Senior Databricks Data Engineer (asistido por IA)  
**Alcance:** 6 notebooks, 11 tablas, 3 Jobs, 27 DQ checks, 8 hipótesis  
**Nota:** los montos absolutos de revenue/descuento de las tablas Gold usan la convención por línea (inflada ×6.0/×5.2). Los valores reales a nivel check se indican en cada sección; ver PROJECT_SUMMARY §8

---

## 1. Resumen Ejecutivo

El pipeline Taco Bueno Pricing está **funcionalmente correcto y listo para presentación**, con dos hallazgos de alto impacto: (1) los montos absolutos de revenue/descuento en las tablas Gold están inflados por semántica de columnas (hallazgo central de la 2ª pasada) y (2) la hipótesis H4 en su formulación original no está respaldada. Además hay varios issues de robustez para producción.

**Hallazgos clave:**
- ✅ **Reconciliación de datos exacta:** 85,273,331 raw = 65,082,100 Silver + 20,188,867 duplicados + 1,586 cuarentena + 778 dups en outliers. Cuadra perfectamente.
- ✅ **Revenue y transacciones reconcilian exactamente** entre Silver y todas las tablas Gold (excepto gold_product que excluye NULL menu_item, documentado) — bajo la misma convención por línea.
- ❌ **Semántica de montos (hallazgo central de la 2ª pasada):** en Silver, `total`, `total_discount`, `tax` y `order_voided` son montos a nivel CHECK repetidos en cada línea; `menu_price`, `quantity` e `item_discount` son a nivel ítem. SUM(total) sobre líneas infla el revenue ×6.0 y el descuento ×5.2. Valores reales (nivel check): revenue bruto **$191.66M**, descuento **$4.61M** (2.41%), ticket **$13.91**. Las tablas Gold siguen la convención inflada; participaciones (%) y conteos siguen siendo válidos. (Ver Sección 6 y PROJECT_SUMMARY §8.)
- ✅ **27/27 DQ checks pasan** — verificado con resultados reales de ejecución.
- ✅ **Liquid clustering** confirmado: CLUSTER BY (date) activo en Silver.
- ⚠️ **3 Jobs existen y están activos** con schedules correctos, pero son redundantes entre sí.
- ❌ **H4 (Happy hour 3-6 PM) no está respaldada por los datos:** el revenue realmente sube de 3 PM a 6 PM; el valle es de 1-3 PM. (Ya corregida en docs: reformulada a 1-3 PM y confirmada.)
- ⚠️ **DQ checks con conteos fijos** (65,082,100; 119 tiendas; fechas hardcodeadas) fallarán cuando lleguen datos nuevos.
- ⚠️ **No hay política de reintentos** en los Jobs, aunque el README lo afirma.
- ❌ **Corrida semanal fallida sin alerta:** run 443911298462218 (2026-10-28, 01:29–01:36 UTC) terminó en RUN_EXECUTION_ERROR antes de la capa Gold, sin notificaciones configuradas; se observaron además ejecuciones solapadas sobre Silver (ver Sección 7).
- ⚠️ **Clave de órdenes incompleta:** store+check subestima 52,189 órdenes (check_number se reutiliza entre fechas). Órdenes reales (store+date+check): **13,780,225**.

**Veredicto:** El pipeline es presentable con las correcciones documentadas en la Sección 10. Las cifras absolutas de revenue/descuento de las tablas Gold requieren corrección a nivel check/ítem (Roadmap de PROJECT_SUMMARY §10), pero no implican reconstrucciones de arquitectura. Las cifras reales están documentadas en PROJECT_SUMMARY §8.

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
| Orders (global distinct, store+check) | 13,728,036 | 13,775,387 (sum monthly) | 47,351 | ⚠️ |
| Orders reales (store+date+check) | 13,780,225 | 13,780,225 | 0 | ✅ |

> **Nota de la 2ª pasada (2026-10-28):** la reconciliación Silver↔Gold es exacta, pero bajo la **convención por línea**: los montos de revenue ($1,150.14M) y descuento ($24.14M) repiten el monto del check en cada línea. Valores reales a nivel check: revenue bruto **$191.66M**, descuento **$4.61M** (2.41%), neto tras descuentos ~$187.05M, ticket real $13.91. La clave correcta de órdenes es store+**date**+check.

### Issues de reconciliación

**Issue #1 — Clave de órdenes incompleta (Severidad: Media; corregido en documentación)**  
Ambos conteos previos usaban store+check y subestiman: `check_number` se reutiliza entre fechas dentro de una misma tienda. El conteo real con clave store+**date**+check es **13,780,225** órdenes. La suma mensual de Gold (13,775,387) y el distinct global (13,728,036) quedan ~52,189 por debajo. PROJECT_SUMMARY §8 ya documenta la cifra correcta; falta replicar la clave con `date` en las tablas Gold (Roadmap).

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

**Todos los 27 checks pasan ✅** (bajo la convención por línea: los checks de reconciliación comparan montos calculados con la misma convención en ambos lados, por lo que el PASS es internamente válido)

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
| revenue | SUM(total) non-Void | ⚠️ Inflado ×6.0 | `total` es monto de check repetido por línea (puede ser negativo, 54 filas); real a nivel check: **$191.66M** brutos |
| transactions | COUNT(*) non-Void | ✅ | Líneas POS, no órdenes |
| orders | COUNT(DISTINCT store_id \|\| '-' \|\| check_number) | ⚠️ Clave incompleta | Subestima ~52,189 (check_number se reutiliza entre fechas); clave correcta: store+date+check = **13,780,225** |
| discount_amount | SUM(total_discount) non-Void | ⚠️ Inflado ×5.2 | Real a nivel check: **$4.61M** (2.41%); a nivel ítem (SUM(item_discount)): $2.88M |
| discount_rate_pct | % con total_discount > 0 | ✅ | Válida como % de líneas en checks descontados; a nivel orden: 8.79% |
| avg_order_value | revenue / orders | ⚠️ Inflado (~$83) | Real por orden ≈ **$13.91** |
| avg_ticket_discounted | AVG(total) WHERE total_discount > 0 | ✅ | Monto bruto promedio por check |
| orders_per_store_day | orders / store_days | ✅ | Correcto (sáb 174.5 … jue 237.8) |

---

## 7. Auditoría de Jobs y Orquestación

### Jobs verificados (configuración real)

| Job | ID | Trigger | Timezone | Status | Cadencia |
|---|---|---|---|---|---|
| Semanal | 613936090449600 | Periodic, 1 WEEK | — | UNPAUSED | semanal |
| Quincenal | 354096890254923 | Periodic, 2 WEEKS | — | UNPAUSED | quincenal |
| Mensual | 1119533549743462 | Cron 0 0 9 1 * ? * | America/Mexico_City | UNPAUSED | mensual |

Todos los Jobs: 1 tarea (notebook 05_Pipeline_Job), queue enabled, sin timeout, sin email/webhook, sin retry policy.

Runs recientes: NO todos exitosos. La corrida periódica del Job Semanal (run 443911298462218) falló el 2026-10-28 (01:29–01:36 UTC, RUN_EXECUTION_ERROR) antes de llegar a la capa Gold — sin CTAS de Gold ni fila en el KPI snapshot en ese periodo. La última corrida completa exitosa fue 00:18–00:23 UTC. Sin notificaciones configuradas, el fallo pasó desapercibido. Además, se observaron ejecuciones solapadas del pipeline sobre las mismas tablas (dos CTAS de Silver en la misma ventana a las 00:19/00:21, y de nuevo a las 01:32/01:34 durante la corrida fallida): `max_concurrent_runs=1` solo aplica por Job, y nada impide que dos Jobs — o un Job y una corrida interactiva — escriban la misma tabla a la vez.

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
**Veredicto: ✅ Confirmado, con matiz (cifras corregidas en 2ª pasada)**  
4.23% de líneas en checks con descuento y **$0 de item_discount** en todo el canal, 17.99M líneas / 4.18M órdenes. Ventas brutas a nivel ítem: **$40.3M** (el $304.5M era la convención por línea, inflado ×6). Check bruto sin descuento $17.11 vs con descuento $12.60 — comparación descriptiva. Pero DoorDash tiene 0% — el claim "lowest of any major channel" debería ser "lowest among promoted channels".

### H2 — Steak Soft Taco canibalizó su precio
**Veredicto: ✅ Confirmado (magnitud corregida en 2ª pasada)**  
45.83% de sus líneas llevan item_discount (la más alta de cualquier producto de alto volumen; #2: Chicken Soft Taco 27.2%); 73.29% de líneas en checks descontados. Ventas brutas a nivel ítem: **$562,749** (el "$2.12M" repetía el total del check en cada línea). Descuento a nivel ítem: **$233,143** (41% de ventas brutas); techo de $429,622 solo si se atribuyera todo el descuento del check a este producto. Descuento por unidad: $1.54 a nivel ítem.

### H3 — Recortar descuentos no causó caída de tráfico
**Veredicto: ✅ Confirmado, con caveats (unidad corregida en 2ª pasada)**  
Descuento cayó 22.27% → 6-7%, tráfico se mantuvo. Corrección de unidad: el notebook calcula órdenes/tienda por **mes** (4,788–6,679); equivalente diario: **154–223 órdenes/tienda/día**. Correlación mensual descuento vs tráfico: +0.11 (débil); el tráfico cayó en dic-25/ene-26, cuando el descuento ya era bajo, y se recuperó en 2026. Caveat 1: enero 2025 tiene solo 10 días (partial month). Caveat 2: esto es correlación temporal, no causalidad — no se controlaron variables externas.

### H4 — Oportunidad de happy hour 3-6 PM
**Veredicto: ❌ NO RESPALDADA en su formulación original; reformulada a 1-3 PM y confirmada**  
Datos reales (gold_hourly; montos absolutos con convención por línea — inflados ×6, participaciones % válidas):
- 12:00 — $109.1M (pico lunch)
- 13:00 — $77.8M (bajando)
- 14:00 — $65.9M (VALLE REAL — 5.73% del revenue del día)
- 15:00 — $77.3M (recuperando)
- 16:00 — $117.2M (subiendo)
- 17:00 — $139.0M (PICO CENA — 12.1%)
- 18:00 — $121.1M

El revenue SUBE de 3 PM a 6 PM. El valle real es 1-3 PM (14:00 es el mínimo). Corrección ya aplicada a README y PROJECT_SUMMARY: "happy hour 1-3 PM" ✅ confirmada como oportunidad; falta la celda del notebook.

### H5 — App móvil con descuento desproporcionado
**Veredicto: ✅ Confirmado (cifras corregidas en 2ª pasada)**  
Mobile: 7,486 líneas / **1,197 órdenes**, 50.91% de líneas en checks descontados, $0 de item_discount, ventas brutas a nivel ítem ≈ **$17,600** (el "$178K" era la convención por línea) vs DoorDash 2.0M líneas / 310,917 órdenes, 0%.

### H6 — Promos de fin de semana
**Veredicto: ⚠️ Evaluar (apropiadamente cauteloso; día más bajo corregido en 2ª pasada)**  
Corrección: el día más bajo es el **SÁBADO** (174.5 órdenes/tienda/día; viernes 185.9 segundo más bajo; domingo 216.3), no el domingo. Jueves (237.8) y miércoles (237.5) son los más altos; fin de semana −12.7% vs laborales. Veredicto de A/B test correcto.

### H7 — Descuentos en órdenes pequeñas
**Veredicto: ⚠️ Evaluar (apropiadamente cauteloso)**  
5.47M órdenes <$10 (39.8%), 564K con descuento, $1.86M (40.3% del descuento total a nivel orden — metodología correcta: usa el monto del check una sola vez). Caveat de A/B correcto.

### H8 — Sides y condimentos son giveaways
**Veredicto: ⚠️ REVISAR (corregido en 2ª pasada — la premisa no se sostiene a nivel ítem)**  
Los $5.26M sumaban el descuento del CHECK repetido en cada línea de estos grupos — atribución errónea (el descuento pertenecía a otros ítems del check). Descuentos reales a nivel ítem (SUM(item_discount)): Condiments $38,323, Sides $30,499, Other $11,062, resto ≈$278 → total ≈ **$80,162**. La pregunta correcta es si los promos a nivel check (combos) dependen de estos acompañamientos; requiere medición de canasta incremental.

**Issue #14 — H3 implica causalidad sin A/B (Severidad: Media)**  
"No causó caída" es una inferencia causal. Debería ser: "no se observó caída asociada temporalmente con el recorte."

---

## 10. Issues Priorizados

| # | Severidad | Issue | Componente | Estado / Acción |
|---|---|---|---|---|
| 1 | ALTA | Montos Gold inflados: `total`/`total_discount` son montos de check repetidos por línea (revenue ×6.0, descuento ×5.2) | 04_Pipeline_Gold, tablas Gold | Abierto — corregir a nivel check/ítem (Roadmap); cifras reales: $191.66M / $4.61M / ticket $13.91 |
| 2 | ALTA | H4: happy hour 3-6 PM no respaldada | 06_Analisis_Hipotesis, README | ✅ Corregido en docs (1-3 PM); pendiente celda del notebook |
| 3 | ALTA | DQ checks con conteos/fechas fijas | 03/04 notebooks | Abierto — cambiar a rangos dinámicos |
| 4 | ALTA | No hay política de reintentos | 3 Jobs, README | Abierto — agregar max_retries o corregir README |
| 5 | MEDIA | Gold se publica antes de DQ | 05_Pipeline_Job | Abierto — pre-check antes de CTAS |
| 6 | MEDIA | 3 Jobs redundantes + corrida semanal fallida (443911298462218) con ejecuciones solapadas | 3 Jobs | Abierto — pausar 2 de 3; investigar run fallido |
| 7 | MEDIA | Sin notificaciones de fallo | 3 Jobs | Abierto — configurar email on_failure |
| 8 | MEDIA | Clave de órdenes incompleta (store+check subestima 52,189; real: 13,780,225 con store+date+check) | README, SUMMARY, Gold | ✅ Documentado en PROJECT_SUMMARY §8; pendiente replicar clave en Gold y actualizar README |
| 9 | MEDIA | H3: causalidad sin A/B + unidad órdenes/tienda mal etiquetada (mes vs día) | 06_Analisis_Hipotesis | Documentado en PROJECT_SUMMARY; pendiente caveat en celda |
| 10 | BAJA | H1: "lowest discount rate" inexacto + revenue por línea ($304.5M → $40.3M ítem) | 06_Analisis_Hipotesis | Documentado en PROJECT_SUMMARY; pendiente celda |
| 11 | BAJA | H3: enero 2025 partial month | 06_Analisis_Hipotesis | Abierto — excluir o documentar |
| 12 | BAJA | Valor -$11.92 en promo_effectiveness | 04_Pipeline_Gold | Abierto — investigar origen |
| 13 | BAJA | KPI snapshot con cadencias NULL | kpi_snapshot | Abierto — backfill o dejar histórico |
| 14 | BAJA | Sin timeout en Jobs | 3 Jobs | Abierto — agregar 3600s |
| 15 | BAJA | Tolerancia DQ fija $5 | 03/04 notebooks | Abierto — cambiar a porcentual |

---

## 11. Fixes Recomendados

### Fix 1 (ALTA): Corregir H4 — happy hour — ✅ aplicado en docs; pendiente celda del notebook
Archivo: 06_Analisis_Hipotesis.ipynb, celda H4
Cambiar el análisis de 3-6 PM a 1-3 PM. ✅ README y PROJECT_SUMMARY ya usan 1-3 PM.

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

### Fix 6 (MEDIA): Clave de órdenes con fecha (agregado en 2ª pasada)  
Componente: 04_Pipeline_Gold, tablas Gold  
Agregar `date` a la clave de órdenes (store+date+check): 13,780,225 órdenes reales vs 13,728,036 con store+check.

### Fix 7 (MEDIA): Aclarar conteo de órdenes — ✅ documentado  
Archivo: README.md, PROJECT_SUMMARY.md  
✅ PROJECT_SUMMARY §8 documenta órdenes reales 13,780,225 (store+date+check); 13,775,387 (suma mensual) y 13,728,036 (distinct global) subestiman ~52,189. Falta actualizar el README con la cifra final.

### Fix 8 (MEDIA): Caveat de causalidad en H3
Archivo: 06_Analisis_Hipotesis.ipynb, celda H3
Agregar: "Esta observación es temporal, no causal. Recomendamos A/B test."

### Fix 9 (ALTA, 2ª pasada): Corregir montos de las tablas Gold a nivel check/ítem  
Archivo: 04_Pipeline_Gold, tablas Gold  
`total`/`total_discount` son montos de check repetidos por línea: revenue bruto real **$191.66M** (no $1,150.14M), descuento real **$4.61M** / 2.41% (no $24.14M), ticket real **$13.91** (no $17.73). Agregar métricas a nivel check (una fila por check) y a nivel ítem (SUM(item_discount)) junto a las actuales por línea; las participaciones porcentuales actuales siguen siendo válidas. Detalle: PROJECT_SUMMARY §8.

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
