# Datalithics — Taco Bueno Pricing Analytics
## Resumen Ejecutivo del Proyecto (Fase 2)

---

## 1. ¿De qué trata el proyecto?

Taco Bueno es una cadena de restaurantes de comida rápida con 119 tiendas en EE.UU. El equipo de pricing tomó una decisión en enero 2025: **recortar agresivamente los descuentos** (de 22.27% a ~6-7%). El objetivo de este proyecto es evaluar si esa decisión fue correcta, medir el impacto en tráfico y revenue, y recomendar estrategias de descuento basadas en datos.

**Dataset:** 597 archivos CSV diarios con 85,273,331 filas de transacciones POS (enero 2025 – septiembre 2026), 22 columnas, todo en STRING.

**Pregunta central:** ¿Los descuentos en Taco Bueno son efectivos, o son una fuga de revenue sin impacto en tráfico?

---

## 2. Arquitectura del Pipeline (Medallion)

```
  597 CSVs diarios → UC Volume → taco_bueno_raw_data (85.3M filas, 22 STRING cols)
         │
         ▼
  ┌─── BRONZE ───┐
  │ taco_bueno_raw_data (85,273,331 filas)
  └───────────────┘
         │  Notebook 03_Pipeline_Silver
         │  • Deduplicación exacta (20.2M filas duplicadas por CSVs solapados)
         │  • Conversión de tipos (STRING → INT, DATE, DOUBLE, TIMESTAMP)
         │  • Filtros de outliers (quantity > 50, menu_price > $50)
         │  • Cuarentena de registros inválidos (1,586 filas)
         │  • 12 DQ checks (validación automática)
         ▼
  ┌─── SILVER ───┐
  │ taco_bueno_silver (65,082,100 filas · CLUSTER BY date)
  │ taco_bueno_silver_quarantine (1,586 outliers trazables)
  └───────────────┘
         │  Notebook 04_Pipeline_Gold
         │  • 6 tablas analíticas agregadas
         │  • 15 DQ checks de reconciliación Silver ↔ Gold
         ▼
  ┌─── GOLD ───┐
  │ taco_bueno_gold_monthly       (21 filas — una por mes)
  │ taco_bueno_gold_product       (643 filas — una por producto)
  │ taco_bueno_gold_channel       (12 filas — una por canal)
  │ taco_bueno_gold_hourly        (24 filas — una por hora)
  │ taco_bueno_gold_dayofweek     (7 filas — una por día de la semana)
  │ taco_bueno_gold_promo_effectiveness (product × channel)
  └────────────┘
         │
         ▼
  Notebook 05_Pipeline_Job (orquestador)
  • Ejecuta 03 → 04 secuencialmente
  • Verifica 27 DQ checks (asserts)
  • Guarda KPI snapshot por cada corrida
  • 3 Jobs programados: semanal, quincenal, mensual
```

### Transformaciones clave (capa por capa)

**Bronze → Silver (notebook 03):**

| Paso | Qué hace | Cuánto afecta |
|---|---|---|
| Deduplicación exacta | `SELECT DISTINCT` elimina filas repetidas por solapamiento de CSVs | 20,188,867 filas válidas eliminadas (23.68%) + 778 copias duplicadas de outliers (ver reconciliación) |
| Conversión de tipos | CAST STRING → INT/DATE/DOUBLE/TIMESTAMP con ANSI desactivado (NULL en vez de error) | 22 columnas STRING → 19 columnas tipadas |
| Drop de columnas 100% nulas | `time_card`, `menu_item_ext_ref`, `total_rounding` | 3 columnas eliminadas |
| Filtro de outliers POS | `quantity > 50` (salsas con qty=3,996) y `menu_price > $50` (donaciones, gift cards) | 1,586 filas únicas a cuarentena |
| Exclusión de Voids | `transaction_type = 'Void'` se excluye del análisis (Gold) pero se conserva en Silver | 219,311 filas Void tras dedup (224,280 en raw incluyendo duplicados) |

**Silver → Gold (notebook 04):**

| Tabla Gold | Grain | Propósito | Valor de negocio |
|---|---|---|---|
| `gold_monthly` | 1 fila = 1 mes | Tendencia de revenue, descuentos y tráfico | ¿El recorte de descuentos afectó el tráfico mensual? |
| `gold_product` | 1 fila = 1 producto | Desempeño y descuento por producto | ¿Qué productos están sobre-descuentados? |
| `gold_channel` | 1 fila = 1 canal | Volumen y descuento por canal | ¿Qué canales no necesitan promociones? |
| `gold_hourly` | 1 fila = 1 hora | Ventas por hora del día | ¿Hay oportunidad de happy hour? |
| `gold_dayofweek` | 1 fila = 1 día | Tráfico por día de la semana | ¿Los fines de semana necesitan promos? |
| `gold_promo_effectiveness` | 1 fila = producto × canal | Efectividad de descuento por combinación | ¿Dónde el descuento no genera valor? |

### Reconciliación aritmética Raw → Silver (verificada contra las tablas, 2026-10-28)

```
85,273,331 raw = 65,082,100 Silver (únicos válidos)
              + 20,188,867 duplicados de filas válidas
              +    1,586 outliers únicos (cuarentena)
              +      778 copias duplicadas de outliers   ← la brecha antes no explicada
              +        0 filas con CAST inválido (Quantity/Menu_Price)
```

La diferencia de **778 filas** son copias duplicadas de registros outlier: fallan los filtros de outliers y además están duplicadas, por lo que no aparecen ni en Silver ni en la cuarentena (que guarda solo outliers únicos). No hay filas perdidas por conversiones de tipo (0 filas con `CAST` inválido) ni ninguna exclusión adicional.

---

## 3. Data Quality (27 checks automatizados)

Cada corrida del pipeline ejecuta 27 validaciones automáticas. Si cualquier check falla, el Job termina en ERROR.

**Silver (12 checks):**
- Volumen esperado: 65,082,100 filas (igual que la tabla limpia de Fase 1)
- 0 duplicados restantes después de DISTINCT
- 0 fechas nulas
- 0 registros con quantity fuera de rango (0–50)
- 0 registros con menu_price > $50
- 0 transaction_type fuera del dominio (Regular Sale, Discount, Void)
- Rango de fechas: 2025-01-22 a 2026-09-27
- Reconciliación aritmética exacta: raw = únicos Silver + duplicados + outliers

**Gold (15 checks):**
- Reconciliación de transactions, revenue y orders entre cada tabla Gold y la Silver
- Todas las tablas Gold existen y tienen filas
- Los totales de Silver = suma de los totales de Gold

**Advertencias de auditoría (2026-10-28):**
- Verificado contra las tablas: las 27 filas de `taco_bueno_dq_results` están presentes y en PASS, con timestamps de la última corrida completa exitosa; la reconciliación Silver↔Gold cuadra exactamente en transacciones y dentro de la tolerancia (±$5) en revenue.
- El check `volumen_silver` exige exactamente 65,082,100 filas y el notebook 03 lo respalda con un `assert` duro: si la fuente recibe CSVs nuevos, el pipeline **fallará por diseño** en vez de procesarlos. Es un detector de cambios en la fuente, **no una regla permanente** — actualizar la expectativa cuando cambie el volumen de datos.
- En el KPI snapshot, la columna `dq_fail` cuenta `status <> 'PASS'` (incluye WARN); los asserts reales solo fallan con FAIL.

**Gobernanza:**
- `taco_bueno_dq_results` — bitácora central con layer, regla, valor observado, esperado y status (PASS/WARN/FAIL)
- `taco_bueno_kpi_snapshot` — historial append-only de cada corrida con KPIs globales
- `taco_bueno_silver_quarantine` — registros excluidos con la misma schema (no se eliminan silenciosamente)

---

## 4. Technical Best Practices

| Práctica | Dónde se aplica | Por qué |
|---|---|---|
| **Liquid clustering** | `CLUSTER BY (date)` en Silver | Acelera queries temporales (agregados mensuales, diarios). Adaptativo, sin particionamiento manual |
| **Idempotencia** | `CREATE OR REPLACE TABLE` en Silver y Gold | Reruns seguros: nunca duplica filas ni double-counta revenue |
| **DQ expectations + asserts** | 27 checks + Python asserts en 05_Pipeline_Job | Si un check falla, el Job termina en ERROR antes de escribir el KPI snapshot |
| **Cuarentena** | `taco_bueno_silver_quarantine` | Outliers no se eliminan silenciosamente — quedan trazables para auditoría |
| **ANSI mode off** | `spark.sql.ansi.enabled = false` | CAST devuelve NULL en valores inválidos (no crashea el pipeline); los capturan los DQ checks |
| **Streaming (considerado)** | No implementado hoy | El fuente es estático (597 CSVs). Si fuera streaming, migrar a `CREATE STREAMING TABLE` + Auto Loader |
| **Materialized views (considerado)** | No usadas, se eligió CTAS | CTAS da control del refresh scheduling + DQ gating secuencial. MVs se refrescan asíncronamente, rompiendo el flujo transform→check→assert→snapshot |

---

## 5. Orquestación (Lakeflow Jobs)

| Job | Trigger | Cadencia |
|---|---|---|
| Taco Bueno - Semanal | Cada 1 semana | `semanal` |
| Taco Bueno - Quincenal | Cada 2 semanas | `quincenal` |
| Taco Bueno - Mensual | 1° del mes, 09:00 (CDMX) | `mensual` |

**Manejo de fallos y reruns:**
- Silver falla → Gold no ejecuta (dependencia secuencial vía `%run`)
- DQ check falla → assert termina el Job en ERROR
- Rerun = full refresh idempotente (CTAS reemplaza la tabla completa)
- KPI snapshot es append-only (una fila por corrida, nunca borra historial)
- Si la corrida falla antes del snapshot, no se escribe fila parcial (verificado: la corrida fallida del 28-oct no dejó fila)

**Hallazgos de auditoría sobre los Jobs (2026-10-28):**
- La corrida periódica del Job **Semanal falló** (RUN_EXECUTION_ERROR, 01:29–01:36 UTC) antes de llegar a la capa Gold: no hay CTAS de Gold ni fila en el KPI snapshot en ese periodo. La última corrida completa exitosa fue la de 00:18–00:23 UTC. **No hay notificaciones configuradas** (email/webhook), así que el fallo pasó desapercibido — ver Roadmap.
- Se observaron **ejecuciones solapadas** del pipeline sobre las mismas tablas (dos CTAS de Silver en la misma ventana a las 00:19/00:21, y de nuevo a las 01:32/01:34 durante la corrida fallida). `max_concurrent_runs=1` solo aplica por Job: nada impide que dos Jobs — o un Job y una corrida interactiva — escriban la misma tabla a la vez, con riesgo de conflicto de escritura concurrente en Delta.
- Los **3 Jobs son redundantes entre sí**: ejecutan exactamente el mismo pipeline completo (~5 min, full recompute) sobre una fuente estática (597 CSVs ya cargados). Se crearon como prueba de cadencias; para producción basta mantener uno y pausar los otros dos.

---

## 6. Análisis de Hipótesis (H1–H8)

El notebook `06_Analisis_Hipotesis` valida 8 hipótesis del equipo de pricing usando las tablas Gold.

### H1 — Drive Thru no necesita descuentos

**Estrategia propuesta:** Mantenerlo sin promoción.

**Evidencia:**
- 17,999,093 líneas / 4,178,684 órdenes; ventas brutas a nivel ítem $40.3M (el $304.5M reportado usa la convención por línea, inflada ×6 — ver sección 8)
- Tasa de descuento: **4.23%** de líneas en checks con descuento (la más baja entre canales propios) y **$0 de `item_discount`** en todo el canal
- Check bruto promedio: sin descuento $17.11 vs con descuento $12.60 (gap +$4.51). Nota: son montos brutos por check; comparación descriptiva — los checks con descuento son más pequeños, no prueba que el descuento los reduzca (sin grupo de control)
- Nota: DoorDash tiene 0% de descuento, pero es un canal de terceros sin control de pricing

**Veredicto: ✅ CONFIRMADO** — Drive Thru opera eficientemente sin promociones.

---

### H2 — Steak Soft Taco canibalizó su precio

**Estrategia propuesta:** Revisar su descuento.

**Evidencia:**
- 118,599 líneas / 151,713 unidades; ventas brutas a nivel ítem (menu_price × qty): **$562,749** — el "$2.12M" repetía el total del check en cada línea
- **45.83%** de sus líneas llevan `item_discount` (la más alta de cualquier producto de alto volumen; el #2, Chicken Soft Taco, llega a 27.2%); 73.29% de sus líneas están en checks con descuento
- Descuento a nivel ítem: **$233,143** (41% de sus ventas brutas); techo de $429,622 solo si se atribuyera todo el descuento del check a este producto
- Descuento por unidad: **$1.54** a nivel ítem ($2.83 era la convención por línea)
- Check bruto con descuento ($17.49) vs sin ($19.01) — comparación descriptiva

**Veredicto: ✅ CONFIRMADO (magnitud corregida)** — 1 de cada 2 líneas lleva descuento de ítem (el mayor de la cadena), perdiendo ~$233K a nivel ítem (hasta $429K si se atribuyen los descuentos de check completos).

---

### H3 — Recortar descuentos no causó la caída de tráfico

**Estrategia propuesta:** Buscar otras causas del bajo tráfico.

**Evidencia:**
- Tasa de descuento (% de líneas en checks descontados) cayó de 22.27% (ene 2025) a ~6-7% (2026)
- El notebook calculó órdenes por tienda por **mes** (orders/active_stores): 4,788–6,679 — el resumen previo las etiquetaba "por día" por error. Equivalente diario: **154–223 órdenes/tienda/día** (feb-25 a ago-26, meses completos)
- Correlación mensual descuento vs órdenes/tienda/día: **+0.11 (débil)**
- El tráfico no siguió al descuento: cayó en dic-25/ene-26 (154–158 órdenes/tienda/día, cuando el descuento ya era bajo) y se recuperó en 2026 (~205–212)

**Veredicto: ✅ CONFIRMADO** — El recorte de descuentos fue seguro. El tráfico no dependía de las promociones. (Nota: correlación temporal, no causalidad — no se controlaron variables externas. Se recomienda A/B test.)

---

### H4 — Oportunidad de happy hour (1–3 PM)

**Estrategia propuesta:** Promociones en la ventana de tarde post-lunch.

**Evidencia:**
- El valle es la hora 14:00 (2 PM): **5.73% del revenue del día**, el mínimo del horario 9:00–20:00
- Picos: 11:00 (13.3%) y 17:00 (12.1%); la ventana 13:00–15:59 concentra 19.2% del revenue
- Los montos absolutos de gold_hourly ($65.9M a las 14:00) usan la convención por línea (inflados ×6); las participaciones porcentuales sí son válidas
- Nota: la hipótesis original decía 3-6 PM, pero el revenue SUBE de 3-6 PM (hacia el pico de cena); el valle real es 1-3 PM (la celda usa horas 13–15: 1PM/2PM/3PM)

**Veredicto: ✅ CONFIRMADO** — Existe una ventana de oportunidad para promociones de tarde (1-3 PM) que puede aplanar la curva de ventas.

---

### H5 — App móvil: 50.9% de descuento en solo 7,486 transacciones

**Estrategia propuesta:** Repensar el enfoque digital.

**Evidencia:**
- Mobile: 7,486 líneas / **1,197 órdenes**, ventas brutas a nivel ítem ≈ **$17,600**; **50.91% de líneas en checks con descuento y $0 de item_discount** (los descuentos son promos a nivel check)
- DoorDash: 2,001,300 líneas / 310,917 órdenes, **0% de descuento**
- UberEats: 605,207 líneas / 87,187 órdenes, **0.08% de descuento**
- La app propia concentra más descuento que el resto digital con volumen marginal (el "$178,722 revenue" es la convención por línea)

**Veredicto: ✅ CONFIRMADO** — La estrategia de descuento en la app móvil es desproporcionada. Se necesita repensar el enfoque digital.

---

### H6 — Promos de fin de semana para nivelar el tráfico

**Estrategia propuesta:** Family meals y bundles de fin de semana.

**Evidencia:**
- **Sábado: 174.5 órdenes/tienda/día — el día más bajo** (viernes 185.9 es el segundo más bajo; domingo 216.3)
- Jueves (237.8) y miércoles (237.5) son los más altos
- Fin de semana (promedio sáb+dom: 195.4) vs días laborales (223.9): **−12.7% de tráfico**
- Corrección de auditoría: el resumen previo decía que domingo era el día más bajo — según gold_dayofweek, el día más bajo es sábado

**Veredicto: ⚠️ EVALUAR** — El fin de semana tiene menor tráfico, pero se necesita un A/B test para confirmar que las promos lo nivelan sin canibalizar revenue.

---

### H7 — Descuentos en órdenes pequeñas no compensan el revenue perdido

**Estrategia propuesta:** Limitar descuentos a partir de un umbral mínimo de compra.

**Evidencia:**
- Órdenes <$10: 5,468,856 (39.8% del total) — ticket promedio $6.45
- 10.32% de estas órdenes reciben descuento
- Descuento en órdenes <$10: **$1,857,334** — 40.3% del descuento total **a nivel orden** (~$4.61M; metodología correcta: usa el monto del check una sola vez, a diferencia de las tablas Gold que repiten el descuento del check en cada línea)
- Las órdenes pequeñas concentran un 40% del descuento pero generan poco revenue

**Veredicto: ⚠️ EVALUAR** — Si el bucket pequeño concentra un % de descuento desproporcionado a su revenue, definir un umbral mínimo de compra (ej. $10) y validarlo con A/B.

---

### H8 — Descuentos en sides y condimentos son regalos puros

**Estrategia propuesta:** Restringir item_discount en estos grupos a combos.

**Evidencia:**
- Corrección de auditoría: los $5,261,623 sumaban el descuento del CHECK repetido en cada línea de estos grupos — atribución errónea (el descuento pertenecía a otros ítems del check)
- Descuentos reales a nivel ítem (`SUM(item_discount)`): Condiments **$38,323**, Sides **$30,499**, Other **$11,062**, Substitution $277, Drinks $1, Paper $0, Meat $0 → total ≈ **$80,162**
- (I) Meat: **0% de descuento a nivel ítem** (el 44.05% eran líneas dentro de checks descontados; con ~1,359–1,589 transacciones es ruido de todos modos)

**Veredicto: ⚠️ REVISAR (corregido)** — La premisa "giveaways" no se sostiene a nivel ítem: sides/condimentos casi no reciben descuentos directos (~$80K en total). La pregunta correcta es si los promos a nivel check (combos) dependen de estos acompañamientos; requiere re-análisis con medición de canasta incremental antes de cualquier decisión.

---

## 7. Hallazgos y Resultados Más Importantes

### Hallazgo #1: El recorte de descuentos fue la decisión correcta

La tasa de descuento cayó de 22.27% a ~6-7% y el tráfico no la siguió (correlación mensual +0.11; órdenes/tienda/día 154–223, con caída en dic-25/ene-26 que no coincide con los meses de mayor descuento). Los descuentos no generaban tráfico incremental: **$4.61M a nivel check** (2.4% de las ventas brutas reales de $191.66M). El $24.14M / 2.10% reportado antes es la suma por línea, que multiplica el descuento del check por su número de líneas (~×5.2).

### Hallazgo #2: Steak Soft Taco perdía ~$233K a nivel ítem (hasta $429K con descuentos de check)

45.83% de sus líneas llevan descuento de ítem (el mayor de la cadena; el #2 llega a 27.2%): **$233,143 sobre ventas brutas de ítem de $562,749 (41%)**. Hasta $429,622 si se atribuyeran los descuentos de check completos. El producto más sobre-descuentado de la cadena.

### Hallazgo #3: 40% del descuento va a órdenes de menos de $10

5.47 millones de órdenes (39.8%) con check bruto promedio de $6.45 absorben $1.86M — el 40.3% de todo el descuento a nivel orden. El cálculo es correcto (montos por check); falta validar con A/B que no generen valor incremental.

### Hallazgo #4: Sides y condimentos recibieron ≈$80K reales en descuentos de ítem

Corregido en auditoría: a nivel ítem estos grupos recibieron ≈ **$80K** en descuentos (Condiments $38K, Sides $30K, resto ~$11K). Los $5.26M reportados antes eran descuentos de check atribuidos erróneamente a cada línea de estos grupos. La premisa de "regalo puro" requiere re-análisis.

### Hallazgo #5: Drive Thru no necesita promociones

Con 18M líneas (4.18M órdenes), $40.3M de ventas brutas de ítem y **$0 de item_discount** (solo 4.23% de líneas dentro de checks descontados), Drive Thru es el canal propio más eficiente. Su check bruto sin descuento ($17.11) es mayor que con descuento ($12.60) — comparación descriptiva.

### Hallazgo #6: La app móvil está mal enfocada

50.91% de líneas en checks descontados sobre solo 1,197 órdenes (~$17.6K de ventas brutas de ítem, $0 en item_discount) vs DoorDash con 0% de descuento en 310,917 órdenes. La app propia necesita repensar su estrategia digital.

### Hallazgo #7: Happy hour 1-3 PM es una oportunidad

Existe un valle consistente a las 14:00 (5.73% del revenue del día, el mínimo de 9:00–20:00) vs el pico de cena a las 17:00 (12.1%) y el de lunch a las 11:00 (13.3%). Una promoción dirigida a la ventana 1-3 PM podría aplanar la curva de ventas diarias.

### Hallazgo #8: Los fines de semana tienen menos tráfico

Sábado es el día más bajo (174.5 órdenes/tienda/día vs 237.5–237.8 de miércoles/jueves; fin de semana −12.7% vs laborales). Bundles familiares podrían nivelar el tráfico, pero requieren validación con A/B test.

---

## 8. Métricas Clave del Dataset

| Métrica | Valor |
|---|---|
| Período | 2025-01-22 a 2026-09-27 (21 meses) |
| Transacciones (no-Void, líneas POS) | 64,862,789 |
| Órdenes reales | **13,780,225** (store+date+check). Nota: 13,775,387 (suma mensual) y 13,728,036 (distinct global) usan store+check y subestiman ~52,189 órdenes porque `check_number` se reutiliza entre fechas dentro de una tienda |
| Tiendas | 119 (115 activas máx por mes) |
| Revenue (convención por línea, tablas Gold actuales) | $1,150.14M — **sobreestimado ×6.0**: `total` es el monto del check repetido en cada línea |
| Revenue bruto real (nivel check, pre-descuento, sin impuesto) | **$191.66M** |
| Revenue neto tras descuentos de check | ~$187.05M |
| Ticket promedio real (check bruto por orden) | **$13.91** ($17.73 era el promedio por línea del monto del check) |
| Descuento real (nivel check) | **$4.61M** = 2.41% del bruto (el $24.14M / 2.10% es la suma por línea, ×5.2) |
| Descuento a nivel ítem (`SUM(item_discount)`) | $2.88M |
| Tasa de descuento (ene 2025) | 22.27% de líneas en checks descontados |
| Tasa de descuento (2026) | ~6-7% de líneas; 8.79% de las órdenes tuvieron algún descuento en el periodo |
| Productos | 643 |
| Canales | 12 (To Go + Drive Thru = 63% de las líneas) |
| Filas raw | 85,273,331 |
| Filas Silver | 65,082,100 (después de dedup + filtros) |
| Filas Void en Silver | 219,311 |
| Outliers en cuarentena | 1,586 (+778 copias duplicadas en raw) |
| DQ checks por corrida | 27 (12 Silver + 15 Gold) |
| Corridas registradas | 9 (historial en taco_bueno_kpi_snapshot) |

### Definiciones de métricas (fórmula y unidad de análisis — auditoría 2026-10-28)

**Hallazgo central:** en la Silver, `total`, `total_discount`, `tax` y `order_voided` son montos **a nivel check repetidos en cada línea** (verificado: 0 de 21,638 checks multi-línea de un día muestra presentan `total` distinto entre líneas; ej.: un check con 3 ítems de $4.29/$2.39/$4.49 lleva `total`=$11.17 en las tres líneas, igual a la suma de los ítems). `menu_price`, `quantity` e `item_discount` sí son a nivel ítem. Por eso, toda `SUM(total)` o `SUM(total_discount)` sobre filas multiplica el monto del check por su número de líneas (~4.7 líneas/orden).

| Métrica | Fórmula actual (Gold) | Unidad | Problema / corrección |
|---|---|---|---|
| `transactions` | COUNT(*) filas no-Void | línea POS | OK como "líneas"; no son órdenes |
| `orders` | COUNT(DISTINCT store_id || '-' || check_number) | orden | Subestima 52,189 órdenes: incluir `date` en la clave |
| `revenue` | SUM(total) | $ | Inflado ×6.0. Correcto: una fila por check → **$191.66M brutos** |
| `discount_amount` | SUM(total_discount) | $ | Inflado ×5.2. Correcto: **$4.61M** por check; a nivel ítem: $2.88M |
| `discount_rate_pct` | % de líneas con total_discount > 0 | % líneas | Válida como "líneas dentro de checks descontados"; a nivel orden: 8.79% |
| `avg_ticket` | AVG(total); en KPI: revenue/transactions | $ por línea | Es el check bruto promediado por línea. Real por orden: **$13.91** |
| `avg_order_value` | revenue / orders | $ por orden | Inflado (~$83) por el revenue por línea; real ≈ $13.91 |
| `avg_orders_per_store_day` | SUM(orders) / store-days (dayofweek) | órdenes/tienda/día | Correcto (174.5 sáb … 237.8 jue) |
| `orders_per_store` (H3) | orders / active_stores por mes | órdenes/tienda/**mes** | Antes etiquetado "por día" — corregido |
| Revenue por hora | SUM(total) por HOUR(begin_time) | $ por hora | Absolutos inflados ×6; participaciones (%) válidas |
| `discount_per_unit` (producto) | SUM(total_discount) / SUM(quantity) | $/unidad | Descuento de check sobre unidades del ítem; a nivel ítem: SUM(item_discount)/units |

---

## 9. Estructura del Proyecto

```
Datalithics - Taco Bueno Challenge/
├── README.md                      ← Documentación completa del proyecto
├── PROJECT_SUMMARY.md             ← Este archivo (resumen ejecutivo)
├── LICENSE
├── .gitignore
├── requirements.txt
└── Notebooks/
    ├── 01_Limpieza_Datos.ipynb    ← Fase 1: limpieza y profiling (source de clean_data)
    ├── 02_EDA.ipynb               ← EDA de negocio: 16 gráficas + 8 secciones
    ├── 03_Pipeline_Silver.py      ← Pipeline productivo Bronze → Silver
    ├── 04_Pipeline_Gold.py        ← Pipeline productivo Silver → Gold
    ├── 05_Pipeline_Job.ipynb      ← Orquestador: ejecuta 03→04, DQ asserts, KPI snapshot
    └── 06_Analisis_Hipotesis.ipynb ← Análisis de hipótesis H1-H8 sobre Gold
```

**Tablas en Databricks (workspace.default):**

| Tabla | Capa | Filas | Descripción |
|---|---|---|---|
| `taco_bueno_raw_data` | Bronze | 85,273,331 | Datos crudos en STRING |
| `taco_bueno_silver` | Silver | 65,082,100 | Limpios, tipados, deduplicados |
| `taco_bueno_silver_quarantine` | Silver | 1,586 | Outliers excluidos (trazables) |
| `taco_bueno_gold_monthly` | Gold | 21 | Métricas mensuales |
| `taco_bueno_gold_product` | Gold | 643 | Métricas por producto |
| `taco_bueno_gold_channel` | Gold | 12 | Métricas por canal |
| `taco_bueno_gold_hourly` | Gold | 24 | Métricas por hora del día |
| `taco_bueno_gold_dayofweek` | Gold | 7 | Métricas por día de la semana |
| `taco_bueno_gold_promo_effectiveness` | Gold | — | Efectividad producto × canal |
| `taco_bueno_dq_results` | Governance | — | Bitácora de 27 DQ checks |
| `taco_bueno_kpi_snapshot` | Governance | — | Historial append-only de corridas |

---

## 10. Roadmap

- [x] Auditoría final de métricas y reconciliación de filas (2026-10-28) — ver secciones 2 y 8
- [ ] **Corregir las tablas Gold** para usar montos a nivel check/ítem: revenue (una fila por check), discount_amount (SUM(total_discount) por check o SUM(item_discount) por línea) y orders con clave store+date+check
- [ ] Notificaciones email/webhook cuando un Job falle (la corrida semanal del 28-oct falló y nadie fue alertado)
- [ ] Investigar la corrida fallida del Job Semanal (run 443911298462218) en los logs de la UI de Jobs — consistente con escrituras concurrentes sobre Silver
- [ ] Consolidar la orquestación en UN Job (pausar quincenal y mensual) y evitar corridas interactivas simultáneas con los Jobs
- [ ] Procesamiento incremental en Silver (vs. full recompute)
- [ ] Dashboard Lakeview sobre las tablas Gold
- [ ] Re-análisis de H8 con descuentos a nivel ítem (atribución correcta)
- [ ] Experimentos A/B para validar hipótesis causales (H3, H6, H7)

---

*Fase 2 — Datalithics (Tec de Monterrey). Generado por el pipeline en Databricks.*