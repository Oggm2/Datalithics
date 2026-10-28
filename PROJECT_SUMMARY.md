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
| Deduplicación exacta | `SELECT DISTINCT` elimina filas repetidas por solapamiento de CSVs | 20,188,867 filas eliminadas (23.68%) |
| Conversión de tipos | CAST STRING → INT/DATE/DOUBLE/TIMESTAMP con ANSI desactivado (NULL en vez de error) | 22 columnas STRING → 19 columnas tipadas |
| Drop de columnas 100% nulas | `time_card`, `menu_item_ext_ref`, `total_rounding` | 3 columnas eliminadas |
| Filtro de outliers POS | `quantity > 50` (salsas con qty=3,996) y `menu_price > $50` (donaciones, gift cards) | 1,586 filas únicas a cuarentena |
| Exclusión de Voids | `transaction_type = 'Void'` se excluye del análisis pero se conserva en Silver | 224,280 filas no-Void base |

**Silver → Gold (notebook 04):**

| Tabla Gold | Grain | Propósito | Valor de negocio |
|---|---|---|---|
| `gold_monthly` | 1 fila = 1 mes | Tendencia de revenue, descuentos y tráfico | ¿El recorte de descuentos afectó el tráfico mensual? |
| `gold_product` | 1 fila = 1 producto | Desempeño y descuento por producto | ¿Qué productos están sobre-descuentados? |
| `gold_channel` | 1 fila = 1 canal | Volumen y descuento por canal | ¿Qué canales no necesitan promociones? |
| `gold_hourly` | 1 fila = 1 hora | Ventas por hora del día | ¿Hay oportunidad de happy hour? |
| `gold_dayofweek` | 1 fila = 1 día | Tráfico por día de la semana | ¿Los fines de semana necesitan promos? |
| `gold_promo_effectiveness` | 1 fila = producto × canal | Efectividad de descuento por combinación | ¿Dónde el descuento no genera valor? |

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
- Si la corrida falla antes del snapshot, no se escribe fila parcial

---

## 6. Análisis de Hipótesis (H1–H8)

El notebook `06_Analisis_Hipotesis` valida 8 hipótesis del equipo de pricing usando las tablas Gold.

### H1 — Drive Thru no necesita descuentos

**Estrategia propuesta:** Mantenerlo sin promoción.

**Evidencia:**
- 17,999,093 transacciones, $304.5M revenue
- Tasa de descuento: **4.23%** (la más baja de cualquier canal importante)
- Ticket sin descuento ($17.11) es **mayor** que con descuento ($12.60)
- Gap de ticket: +$4.51 (el descuento reduce el ticket, no lo aumenta)

**Veredicto: ✅ CONFIRMADO** — Drive Thru opera eficientemente sin promociones.

---

### H2 — Steak Soft Taco canibalizó su precio

**Estrategia propuesta:** Revisar su descuento.

**Evidencia:**
- 118,599 transacciones, $2.12M revenue
- Tasa de descuento: **73.29%** (la más alta de cualquier producto de alto volumen)
- Descuento total: **$429,622** (20.2% del revenue del producto)
- Descuento por unidad: $2.83
- Ticket con descuento ($17.49) vs sin descuento ($19.01) — el descuento solo reduce el precio en $1.52

**Veredicto: ✅ CONFIRMADO** — El producto estaba descontando 3 de cada 4 transacciones, perdiendo $429K sin justificación de volumen.

---

### H3 — Recortar descuentos no causó la caída de tráfico

**Estrategia propuesta:** Buscar otras causas del bajo tráfico.

**Evidencia:**
- Tasa de descuento cayó de 22.27% (ene 2025) a 6-7% (2026)
- Órdenes por tienda por día se mantuvieron estables: 4,788–6,778
- No hay correlación visible entre descuento y tráfico
- Tráfico se mantuvo incluso cuando el descuento se redujo 70%

**Veredicto: ✅ CONFIRMADO** — El recorte de descuentos fue seguro. El tráfico no dependía de las promociones.

---

### H4 — Oportunidad de happy hour (3–6 PM)

**Estrategia propuesta:** Promociones en la ventana de tarde.

**Evidencia:**
- Revenue por hora muestra un valle entre 3-6 PM
- Las horas pico son almuerzo (12-14) y cena (18-20)
- 3-6 PM genera menos revenue per hour que cualquier otra ventana del día

**Veredicto: ✅ CONFIRMADO** — Existe una ventana de oportunidad para promociones de tarde que puede aplanar la curva de ventas.

---

### H5 — App móvil: 50.9% de descuento en solo 7,486 transacciones

**Estrategia propuesta:** Repensar el enfoque digital.

**Evidencia:**
- Mobile: 7,486 transacciones, $178,722 revenue, **50.91% de descuento**
- DoorDash: 2,001,300 transacciones, $55.4M revenue, **0% de descuento**
- UberEats: 605,207 transacciones, $18.3M revenue, **0.08% de descuento**
- La app propia tiene más descuento que todos los canales digitales juntos, pero prácticamente cero volumen

**Veredicto: ✅ CONFIRMADO** — La estrategia de descuento en la app móvil es desproporcionada. Se necesita repensar el enfoque digital.

---

### H6 — Promos de fin de semana para nivelar el tráfico

**Estrategia propuesta:** Family meals y bundles de fin de semana.

**Evidencia:**
- Domingo: 216 órdenes/tienda/día (el más bajo)
- Miércoles: 238 órdenes/tienda/día (el más alto)
- Fines de semana (sábado + domingo) generan menos tráfico que los días de semana
- Diferencia: ~10% menos tráfico en domingo vs miércoles

**Veredicto: ⚠️ EVALUAR** — El fin de semana tiene menor tráfico, pero se necesita un A/B test para confirmar que las promos lo nivelan sin canibalizar revenue.

---

### H7 — Descuentos en órdenes pequeñas no compensan el revenue perdido

**Estrategia propuesta:** Limitar descuentos a partir de un umbral mínimo de compra.

**Evidencia:**
- Órdenes <$10: 5,468,856 (39.8% del total) — ticket promedio $6.45
- 10.32% de estas órdenes reciben descuento
- Descuento total en órdenes <$10: **$1,857,334** (40.3% de TODO el descuento del dataset)
- Las órdenes pequeñas concentran un 40% del descuento pero generan poco revenue

**Veredicto: ⚠️ EVALUAR** — Si el bucket pequeño concentra un % de descuento desproporcionado a su revenue, definir un umbral mínimo de compra (ej. $10) y validarlo con A/B.

---

### H8 — Descuentos en sides y condimentos son regalos puros

**Estrategia propuesta:** Restringir item_discount en estos grupos a combos.

**Evidencia:**
- Condimentos: 6.36M transacciones, $3.06M descontados (9.60% de líneas con descuento)
- Sides: 2.92M transacciones, $1.11M descontados (7.64%)
- Paper/Other: 3.88M transacciones, $0.97M descontados
- Total descontado en sides/condimentos/no-comida: **$5,261,623**
- (I) Meat tiene 44.05% de descuento — pero solo 1,589 transacciones (ruido)

**Veredicto: ⚠️ EVALUAR** — Son "giveaways" si el descuento no genera unidades incrementales. Recomendar restringir a combos y medir canasta incremental.

---

## 7. Hallazgos y Resultados Más Importantes

### Hallazgo #1: El recorte de descuentos fue la decisión correcta

La tasa de descuento cayó de 22.27% a 6-7% y **el tráfico se mantuvo estable** en ~6,200–6,700 órdenes por tienda por día. Los descuentos no estaban generando tráfico incremental — eran una fuga de revenue de **$24.14M** (2.10% del revenue total).

### Hallazgo #2: Steak Soft Taco perdía $429K

Con 73.29% de descuento, este producto perdía $429,622 — el equivalente al 20% de su revenue. Era el producto más sobre-descuentado de toda la cadena.

### Hallazgo #3: 40% del descuento va a órdenes de menos de $10

5.5 millones de órdenes con ticket promedio de $6.45 absorben $1.86M en descuentos. Estos descuentos no generan valor incremental porque el ticket ya es muy bajo.

### Hallazgo #4: Sides y condimentos recibieron $5.26M en descuentos

Combinados, condimentos, sides, paper y substitutions recibieron $5.26M en descuentos — probablemente regalos puros sin impacto en canasta incremental.

### Hallazgo #5: Drive Thru no necesita promociones

Con 18M transacciones, $304.5M revenue y solo 4.23% de descuento, Drive Thru es el canal más eficiente. Su ticket sin descuento ($17.11) es mayor que con descuento ($12.60).

### Hallazgo #6: La app móvil está mal enfocada

50.91% de descuento en 7,486 transacciones ($178K revenue) vs DoorDash con 0% de descuento en 2M transacciones ($55.4M revenue). La app propia necesita repensar su estrategia digital.

### Hallazgo #7: Happy hour 3-6 PM es una oportunidad

Existe un valle consistente en revenue entre 3-6 PM. Una promoción dirigida a esta ventana podría aplanar la curva de ventas diarias.

### Hallazgo #8: Los fines de semana tienen menos tráfico

Domingo es el día más bajo (216 órdenes/tienda/día vs 238 del miércoles). Bundles familiares podrían nivelar el tráfico, pero requieren validación con A/B test.

---

## 8. Métricas Clave del Dataset

| Métrica | Valor |
|---|---|
| Período | 2025-01-22 a 2026-09-27 (21 meses) |
| Transacciones (no-Void) | 64,862,789 |
| Órdenes | 13,775,387 |
| Tiendas | 119 (115 concurrentes máx por mes) |
| Revenue total | $1,150.14M |
| Ticket promedio | $17.73 |
| Descuento total | $24.14M (2.10% del revenue) |
| Tasa de descuento (ene 2025) | 22.27% |
| Tasa de descuento (2026) | ~6-7% |
| Productos | 643 |
| Canales | 12 (To Go + Drive Thru = 63% del tráfico) |
| Filas raw | 85,273,331 |
| Filas Silver | 65,082,100 (después de dedup + filtros) |
| Outliers en cuarentena | 1,586 |
| DQ checks por corrida | 27 (12 Silver + 15 Gold) |
| Corridas del Job registradas | 8 (historial en taco_bueno_kpi_snapshot) |

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

- [ ] Notificaciones email/webhook cuando un Job falle
- [ ] Procesamiento incremental en Silver (vs. full recompute)
- [ ] Dashboard Lakeview sobre las tablas Gold
- [ ] Experimentos A/B para validar hipótesis causales (H6, H7, H8)

---

*Fase 2 — Datalithics (Tec de Monterrey). Generado por el pipeline en Databricks.*