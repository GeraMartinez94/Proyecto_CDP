# 📋 FAQ TÉCNICO - CLOUDERA CDP
## Preguntas de Entrevista + Respuestas

---

## **NIVEL 1: CONCEPTOS FUNDAMENTALES**

### P1: ¿Cuál es la diferencia principal entre Cloudera CDP y Hadoop tradicional?

**R:** 
- **Hadoop tradicional** (CDH - Cloudera Distribution Hadoop) era un stack estático en on-premise
- **CDP** es moderno y flexible:
  - ✅ Multicloud (AWS, Azure, GCP)
  - ✅ Microservicios (componentes independientes)
  - ✅ Security-first (Ranger, Kerberos)
  - ✅ Gobernanza integrada (Atlas)
  - ✅ Pay-as-you-go (en cloud)

**En la entrevista:** "CDP es la evolución moderna de Hadoop, diseñada para cloud nativo con mejor seguridad y gobernanza"

---

### P2: ¿Qué es Data Hub en CDP?

**R:**
Data Hub son clústers especializados para diferentes cargas de trabajo.

**Tipos principales:**
1. **Data Engineering Hub** 
   - Componentes: Spark, Airflow, Hive
   - Uso: ETL, transformaciones batch
   
2. **Data Warehouse Hub**
   - Componentes: Impala, Hive
   - Uso: SQL analíticas, queries rápidas
   
3. **Operational Database**
   - Componentes: HBase, Phoenix
   - Uso: Bases de datos operacionales

**En la entrevista:** "Data Hubs son entornos especializados. Para mi caso de energético usaría Data Engineering Hub con Spark y Airflow"

---

### P3: ¿Qué rol juega Apache Airflow en CDP?

**R:**
**Airflow es el orquestador de workflows en CDP**

- Define workflows como DAGs (Directed Acyclic Graphs)
- Cada tarea tiene dependencias claras
- Monitorea ejecución y reintenta si falla
- Agenda trabajos (scheduling)

**Ejemplo real:**
```
[Ingesta] → [Limpieza] → [Transformación] → [Almacenamiento] → [Reporte]
   ↓          ↓             ↓               ↓               ↓
Task 1   Task 2(espera)  Task 3(espera)  Task 4(espera)  Task 5(espera)
```

Airflow asegura que cada tarea espere a la anterior.

**En la entrevista:** "Airflow es el corazón de la orquestación. Crea DAGs que definen el flujo de datos y garantiza dependencias"

---

### P4: ¿Por qué usar Kafka en lugar de escribir directamente a HDFS?

**R:**
**Kafka proporciona:**

1. **Desacoplamiento:** Productor ≠ Consumidor
2. **Buffer:** Absorbe picos de datos
3. **Replay:** Puedo reprocessar datos
4. **Tolerancia a fallos:** Si consumer cae, Kafka mantiene datos
5. **Streaming real:** No necesito esperar batch

**Comparativa:**
```
SIN Kafka (malo):
API → HDFS directo → si falla API, pierde datos

CON Kafka (bueno):
API → Kafka (buffer) → Spark consume cuando pueda
                    → Si Spark cae, Kafka mantiene datos
```

**En la entrevista:** "Kafka actúa como buffer. Desacopla productores de consumidores y proporciona tolerancia a fallos"

---

## **NIVEL 2: ARQUITECTURA Y DISEÑO**

### P5: Explica el flujo ETL típico en CDP

**R:**
```
EXTRACT (E)          TRANSFORM (T)           LOAD (L)
├─ Kafka ingesta     ├─ Spark processing     ├─ Validar calidad
├─ API calls         ├─ Limpieza             ├─ Guardar en Data Lake
├─ Conectores        ├─ Deduplicación        ├─ Actualizar metadatos
└─ JDBC connectors   ├─ Joins/Aggregations   └─ Crear particiones
                     ├─ ML features
                     └─ Enriquecimiento
```

**Ejemplo energético:**
- **E:** Medidores envían consumo cada 15 min a Kafka
- **T:** Spark limpia, agrega por zona, detecta anomalías
- **L:** Guarda en Data Lake particionado por día/zona

**En la entrevista:** "El flujo es Extract → Transform → Load. Kafka ingesta, Spark transforma, Data Lake almacena"

---

### P6: ¿Cómo manejas datos en tiempo real vs batch en CDP?

**R:**
```
BATCH (Airflow scheduled):
- Tareas planificadas (ej: cada noche)
- Datos históricos
- Airflow DAGs

STREAMING (Kafka + Spark Streaming):
- Datos en tiempo real
- Kafka como fuente
- Spark Streaming procesa micro-batches
- Latencia: segundos/minutos
```

**Decisión:**
- Datos **críticos/tiempo real** → Streaming
- Datos **reportes/históricos** → Batch

**Ejemplo:** Medidores energéticos son casi tiempo real → usar Streaming

**En la entrevista:** "Batch para datos históricos (Airflow), Streaming para real-time (Kafka+Spark). CDP soporta ambos"

---

### P7: ¿Qué es Apache Atlas y por qué es importante?

**R:**
**Atlas = Gobernanza + Linaje de datos**

- **Catálogo:** ¿Qué datos existen?
- **Linaje:** ¿De dónde vienen? ¿A dónde van?
- **Metadatos:** Propietario, clasificación, calidad
- **Trazabilidad:** Cumplimiento regulatorio

**Ejemplo:**
```
dataset_ventas (origen: BD1)
    ↓ (transformado por job_limpieza)
dataset_ventas_limpio
    ↓ (usado por reporte_mensual)
dashboard_BI
```

Atlas rastrea todo esto automáticamente.

**En la entrevista:** "Atlas es el catálogo de datos de CDP. Garantiza gobernanza, linaje y cumplimiento"

---

### P8: ¿Cuál es la diferencia entre Ranger y Kerberos?

**R:**
| Componente | Función | Ejemplo |
|---|---|---|
| **Kerberos** | **Autenticación** (¿eres quién dices ser?) | Login con usuario/contraseña |
| **Ranger** | **Autorización** (¿qué puedes hacer?) | "Juan" puede leer tabla_ventas pero no tabla_salarios |

**En la entrevista:** "Kerberos autentica (quién eres), Ranger autoriza (qué puedes hacer)"

---

## **NIVEL 3: SPARK Y TRANSFORMACIONES**

### P9: ¿Qué es mejor: Spark Batch o Spark Streaming?

**R:**
```
SPARK BATCH:
+ Procesamiento de grandes volúmenes
+ Optimizado para query execution
+ Mejor para datos históricos
- Latencia: minutos/horas

SPARK STREAMING:
+ Bajo latencia (segundos)
+ Datos en tiempo real
+ Micro-batches cada X segundos
- Más complejo de debuggear
```

**Decisión:** 
- **Batch:** Reportes diarios, BI, análisis
- **Streaming:** Alertas, real-time dashboards, anomalías

**En la entrevista:** "Batch para volúmenes, Streaming para latencia. En energía usaría streaming para detectar anomalías"

---

### P10: ¿Cómo optimizas un job Spark que está lento?

**R:**
**Checklist de optimización:**

1. **Particionamiento** 
   - Aumentar particiones = más paralelismo
   - Pero no excesivamente (overhead)

2. **Caching/Persistence**
   - Cache DataFrames reutilizados
   - `df.cache()` después de transformación costosa

3. **Eliminación de shuffles**
   - Evitar repartitions innecesarios
   - Usar broadcast joins para tablas pequeñas

4. **Compresión**
   - Parquet con snappy/gzip
   - Reduce I/O

5. **Executor config**
   - Aumentar memory/cores si disponibles
   - `--executor-cores 4 --executor-memory 8G`

**En la entrevista:** "Revisar particiones, caching, shuffles, compresión. Usar Spark UI para identificar bottlenecks"

---

### P11: ¿Cuál es la diferencia entre DataFrame y RDD en Spark?

**R:**
```
RDD (Resilient Distributed Dataset):
- Bajo nivel, más control
- Más lento
- Menos optimizado

DataFrame:
- Alto nivel, más fácil
- Más rápido (optimizer automático)
- Más recomendado
- SQL-like

# Ejemplo:
# RDD (viejo)
rdd = sc.textFile("datos.txt").map(...)

# DataFrame (moderno)
df = spark.read.parquet("datos.parquet")
```

**En la entrevista:** "DataFrames son preferidos. Tienen optimizador automático (Catalyst) que genera queries más eficientes"

---

## **NIVEL 4: CASOS REALES Y PROBLEMAS**

### P12: Un job Spark falla por "OutOfMemory". ¿Cómo lo debuggeas?

**R:**
**Pasos:**

1. **Revisar logs** → ¿Cuál executor falló?
2. **Aumentar memoria** → `--executor-memory 16G`
3. **Reducir particiones** → Menos datos por partition
4. **Cambiar compresión** → Parquet en lugar de CSV
5. **Broadcast join** → Para tablas pequeñas
6. **Eliminar caching** → Libera memoria

**En production:**
```bash
spark-submit \
  --executor-memory 16G \
  --executor-cores 4 \
  --num-executors 10 \
  job.py
```

**En la entrevista:** "Revisar logs, aumentar memoria, reducir particiones, usar broadcast joins, eliminar caching"

---

### P13: ¿Cómo aseguras que datos sensibles no se exponen en CDP?

**R:**
**Capas de seguridad:**

1. **Ranger** → Control de acceso granular
   ```
   Tabla: medidores_energeticos
   Usuario: juan
   Permiso: SELECT en columnas [medidor_id, consumo_kwh]
   (NO acceso a [coordenadas_GPS, cliente_nombre])
   ```

2. **Kerberos** → Autenticación
   ```
   Solo usuarios con ticket válido pueden acceder
   ```

3. **Encriptación** → Datos en tránsito y reposo
   ```
   HDFS: encriptado
   Parquet: encriptado
   Conexiones: TLS/SSL
   ```

4. **Auditoría** → Ranger audit logs
   ```
   Quién accedió qué
   Cuándo
   Éxito/fallo
   ```

5. **Data Masking** → Ocultar datos sensibles
   ```
   DNI: 12345678 → 12***678
   ```

**En la entrevista:** "Ranger para control granular, Kerberos para autenticación, encriptación en tránsito y reposo, auditoría, data masking"

---

### P14: ¿Cómo manejas calidad de datos en CDP?

**R:**
**Framework de calidad:**

1. **Validación en Ingesta**
   ```
   - ¿Formato correcto?
   - ¿Tipos de dato?
   - ¿Rangos válidos?
   ```

2. **Limpieza en Transformación**
   ```
   - Remover duplicados
   - Llenar nulos
   - Outlieres
   ```

3. **Testeo en Pipeline**
   ```
   - Registros antes/después
   - Estadísticas básicas
   - Alertas si < X% datos válidos
   ```

4. **Monitoreo Post-Load**
   ```
   - Freshness: ¿datos actuales?
   - Completitud: ¿todas las columnas?
   - Accuracy: ¿valores esperados?
   ```

**Herramientas:** Great Expectations, DBT, Observability

**En la entrevista:** "Validación en ingesta, limpieza en transformación, testeo automático, monitoreo post-load"

---

### P15: ¿Cómo versionas tus pipelines/jobs?

**R:**
**Control de versión:**

```
📁 proyecto-cdp/
├─ .git
├─ src/
│  ├─ jobs/
│  │  └─ energy_pipeline_v2.py
│  └─ tests/
│     └─ test_energy_pipeline.py
├─ dags/
│  └─ energy_dag.py
├─ requirements.txt
└─ README.md
```

**Workflow:**
1. Escribir código en branch
2. Tests locales
3. PR (pull request) para review
4. Merge a main
5. Deploy automático a CDP
6. Tag version: v1.0.0, v1.1.0

**En la entrevista:** "Git para control de versión, feature branches, pull requests para review, CI/CD para deploy"

---

## **NIVEL 5: PREGUNTAS TRICKY**

### P16: ¿Qué sucede si Airflow scheduler falla?

**R:**
**Problemas:**
- Jobs no se triggean
- Datos no se procesan
- SLA breaches

**Soluciones:**
1. **Alta disponibilidad**
   ```
   Airflow en cluster (no single machine)
   DB backend compartida (PostgreSQL)
   ```

2. **Backup scheduler**
   ```
   Standby que toma control si principal falla
   ```

3. **Monitoreo**
   ```
   Alertas si scheduler muere
   Notificaciones a DevOps
   ```

4. **Recovery**
   ```
   Backfill: rerun tareas faltantes
   ```

**En la entrevista:** "Alta disponibilidad con cluster, DB compartida, scheduler backup, monitoreo y alertas"

---

### P17: ¿Cómo evitas procesamiento duplicado en CDP?

**R:**
**Técnicas:**

1. **Idempotencia**
   ```
   Si job corre 2 veces, resultado = mismo
   Ej: INSERT OVERWRITE en lugar de INSERT
   ```

2. **Checkpointing**
   ```
   Kafka offset tracking: ¿hasta dónde procesé?
   No reprocessar datos viejos
   ```

3. **Deduplicación en Spark**
   ```python
   df.dropDuplicates(['medidor_id', 'timestamp'])
   ```

4. **Particionamiento**
   ```
   Procesar por partición (fecha/zona)
   Si falla, rerun solo esa partición
   ```

**En la entrevista:** "Idempotencia, offset tracking, deduplicación, particionamiento por fecha"

---

### P18: ¿Cómo debuggeas un job que falla solo en production?

**R:**
**Checklist:**

1. **Reproducir localmente**
   - ¿Datos diferentes?
   - ¿Volumen diferente?
   - ¿Permisos?

2. **Revisar logs en CDP**
   ```bash
   yarn logs -applicationId application_XXX
   spark log viewer
   ```

3. **Ejecutar con debugging**
   ```bash
   spark-submit --debug job.py
   ```

4. **Comparar environments**
   ```
   ¿Versiones iguales?
   ¿Configuraciones iguales?
   ¿Datos iguales?
   ```

5. **Añadir logging**
   ```python
   logger.info(f"Procesando {len(df)} filas")
   logger.error(f"Error: {exception}")
   ```

**En la entrevista:** "Reproducir localmente, revisar logs YARN, debugger, comparar environments, logging detallado"

---

### P19: ¿Cuál es tu estrategia de disaster recovery en CDP?

**R:**
**Plan de recuperación:**

1. **Backups regulares**
   ```
   HDFS/Data Lake: snapshots diarios
   Metadata: BD de Hive/Atlas
   ```

2. **Replicación**
   ```
   Data replicación (3 copias default en HDFS)
   Cross-region backup
   ```

3. **Recovery Time Objective (RTO)**
   ```
   ¿Cuánto tiempo puedo estar down? (ej: 1 hora)
   ```

4. **Recovery Point Objective (RPO)**
   ```
   ¿Qué datos puedo perder? (ej: últimos 15 min)
   ```

5. **Plan de failover**
   ```
   Si cluster principal cae:
   - Activar cluster standby
   - Restaurar desde backup
   - Revalidar integridad
   ```

**En la entrevista:** "Backups regulares, replicación, RTO/RPO definidos, failover automático, test recovery periódico"

---

### P20: ¿Cómo migras un pipeline de Hadoop tradicional a CDP?

**R:**
**Estrategia (Lift & Shift):**

```
FASE 1: Evaluación
├─ Inventariar jobs existentes
├─ Identificar dependencias
└─ Cuantificar recursos

FASE 2: Preparación
├─ Instalar CDP
├─ Crear Data Hubs necesarios
└─ Configurar seguridad (Ranger, Kerberos)

FASE 3: Migración
├─ Convertir jobs Hive → Spark (si necesario)
├─ Mover datos a HDFS en CDP
├─ Testear con datos históricos
└─ Crear DAGs Airflow equivalentes

FASE 4: Validación
├─ Comparar outputs (old vs new)
├─ Performance benchmarking
├─ Seguridad & Compliance

FASE 5: Cutover
├─ Switch en fecha específica
├─ Fallback plan ready
└─ Monitoreo intensivo
```

**En la entrevista:** "Evaluación, preparación, migración incremental, validación exhaustiva, cutover controlado"

---

## 🎯 **TIPS PARA LA ENTREVISTA**

### ✅ **Haz:**
- Ser específico con ejemplos
- Mencionar herramientas (Spark, Airflow, Atlas)
- Hablar de gobernanza (Ranger)
- Describir cases reales
- Hacer preguntas de clarificación

### ❌ **No hagas:**
- Respuestas genéricas
- Inventar términos
- Admitir que no sabes sin contexto
- Interrumpir al entrevistador

---

## 📞 **RESPUESTA MODELO PARA LA ENTREVISTA**

**Pregunta:** "¿Cuál es tu experiencia con Cloudera CDP?"

**Respuesta recomendada:**
```
"Tengo experiencia intermedia con CDP. He trabajado en COREBI
en un pipeline de energía que ingesta datos de medidores via Kafka,
los transforma con Spark (limpieza, agregación, detección de anomalías),
los almacena en Data Lake en formato Parquet, y los orquesta con Airflow.

Implementé controles de seguridad con Ranger, registré metadatos
en Atlas para gobernanza, y monitoreo con SLA checks.

También estoy familiarizado con Data Engineering Hub, y he debuggeado
issues de performance optimizando particiones y usando broadcast joins.

Estoy siempre aprendiendo y muy interesado en profundizar más con CDP".
```

**¿Por qué funciona?**
✅ Específico (pipeline real)
✅ Menciona componentes clave
✅ Demuestra amplitud (ingesta, transform, almacenamiento, orquestación, security)
✅ Muestra curiosidad

