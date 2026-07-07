# CLOUDERA CDP - GUÍA COMPLETA

## 📊 ¿Qué es Cloudera CDP?

**Cloudera Data Platform (CDP)** es una plataforma moderna de datos que permite:
- **Ingestar** datos de múltiples fuentes
- **Procesar** datos a escala (batch y streaming)
- **Almacenar** datos de forma segura
- **Analizar** datos en tiempo real
- **Gobernar** datos (seguridad, cumplimiento)

---

## 🏗️ ARQUITECTURA DE CDP

```
┌─────────────────────────────────────────────────────────────┐
│                    CLOUDERA CDP                              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  INGESTA           PROCESAMIENTO        ALMACENAMIENTO       │
│  ────────          ───────────          ──────────────       │
│  • Kafka           • Spark              • Data Warehouse     │
│  • NiFi            • Airflow            • Data Lake          │
│  • Flink           • Hive               • Delta Lake         │
│  • APIs            • Pig                • HDFS               │
│                                                               │
│                    CAPA DE SEGURIDAD & GOBERNANZA           │
│                    • Ranger (control acceso)                │
│                    • Atlas (linaje de datos)                │
│                    • Kerberos (autenticación)               │
│                                                               │
│  ANALYTICS & ML                                              │
│  • Impala (queries rápidas)                                 │
│  • Hue (interfaz)                                           │
│  • ML Runtimes (MLflow, etc)                                │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔑 COMPONENTES PRINCIPALES DE CDP

### **1. DATA HUB** (Centro de procesamiento)
- **Qué es:** Clústers especializados para diferentes cargas de trabajo
- **Tipos:**
  - **Data Engineering Hub:** Spark, Airflow, Hive
  - **Data Warehouse Hub:** Impala, Hive
  - **Streaming Hub:** Kafka, Flink
- **Caso de uso:** Transformaciones ETL, queries analíticas

### **2. DATA WAREHOUSE** (Almacén de datos)
- **Qué es:** Motor de análisis optimizado para queries SQL
- **Tecnología:** Impala (SQL muy rápido)
- **Ventajas:** Queries en segundos, escalable, bajo costo
- **Uso:** BI, reportes, análisis ad-hoc

### **3. OPERATIONAL DATABASE** (BD operacional)
- **Qué es:** Base de datos SQL-on-Hadoop
- **Tecnología:** HBase, Phoenix
- **Caso de uso:** Datos operacionales de aplicaciones

### **4. MACHINE LEARNING** 
- **Qué es:** Runtime para ML
- **Integración:** MLflow, TensorFlow, Scikit-learn
- **Caso de uso:** Modelos predictivos

### **5. GOBERNANZA (Ranger, Atlas)**
- **Ranger:** Control de acceso granular
- **Atlas:** Linaje de datos (de dónde vienen, dónde van)

---

## 🔄 FLUJO TÍPICO DE DATOS EN CDP

```
FUENTES              INGESTA              PROCESAMIENTO        ALMACENAMIENTO      CONSUMO
─────────            ───────              ──────────────       ──────────────      ───────

APIs                 ┌────────┐           ┌──────────┐          ┌────────────┐
Databases ────────→ │ Kafka  │ ─────────→│ Spark    │ ────────→│ Data Lake  │ ─────→ BI
Files                │ NiFi   │           │ Airflow  │          │ Warehouse  │       Analytics
Streams              └────────┘           │ Hive     │          │ Delta Lake │       ML
                                          └──────────┘          └────────────┘
                                                ↓
                                          (Transformaciones)
                                          • Limpieza
                                          • Deduplicación
                                          • Enriquecimiento
```

---

## 🛠️ STACK TECNOLÓGICO DE CDP

### **Ingesta**
- **Kafka:** Streaming en tiempo real
- **NiFi:** Automatización de flujos de datos
- **Flink:** Procesamiento stream
- **APIs/Conectores:** Conectar con sistemas externos

### **Procesamiento**
- **Apache Spark:** Procesamiento distribuido (batch/streaming)
- **Apache Airflow:** Orquestación de workflows
- **Apache Hive:** Queries SQL en Hadoop

### **Almacenamiento**
- **HDFS:** Distributed File System (datos sin procesar)
- **Delta Lake/Iceberg:** Tablas versionadas
- **Snowflake/Redshift:** (Opcional, integración externa)

### **Seguridad**
- **Kerberos:** Autenticación
- **Ranger:** Authorization (quién puede ver qué)
- **Encryption:** Datos en reposo y en tránsito

### **Gobernanza**
- **Apache Atlas:** Catálogo de datos + linaje

---

## 📈 FLUJO ETL TÍPICO EN CDP

```
EXTRACT (E)
├─ Leer datos de fuente (API, DB, archivos)
├─ Validar conexión
└─ Transferir a HDFS/Delta Lake

TRANSFORM (T)
├─ Limpieza de datos
├─ Deduplicación
├─ Join con otras tablas
├─ Agregaciones
└─ Enriquecimiento

LOAD (L)
├─ Guardar en Data Warehouse
├─ Crear particiones
├─ Indexar si es necesario
└─ Actualizar metadatos en Atlas
```

---

## 🔗 COMPARATIVA: CDP vs Alternativas

| Característica | CDP | Databricks | Snowflake | Google BigQuery |
|---|---|---|---|---|
| **Open Source** | Parcialmente | No | No | No |
| **On-Premise** | ✅ Sí | Parcialmente | Solo Cloud | No |
| **Streaming** | Kafka/Flink | Sí | Limitado | Sí |
| **Costo** | Bajo | Medio | Medio-Alto | Bajo |
| **Gobernanza** | Excelente (Atlas) | Buena | Básica | Básica |
| **Control Acceso** | Ranger (granular) | Bueno | Básico | Bueno |

---

## 💡 CASOS DE USO TÍPICOS

### **1. Data Lake - Sector Energético** (Tu caso)
```
Medidores → Kafka → Spark → Data Lake → Dashboard
│                                      └→ ML (predicción consumo)
└─ Atlas (tracking de datos)
```
- **Ingesta:** Datos de medidores en tiempo real
- **Procesamiento:** Agregaciones por usuario, zona, hora
- **Almacenamiento:** Data Lake + Data Warehouse
- **Consumo:** Dashboards, alertas, modelos predictivos

### **2. ETL Batch - Datos Financieros**
```
Multiple DBs → NiFi → Spark (transformación) → Warehouse
```

### **3. Real-Time Analytics - E-commerce**
```
Kafka (eventos) → Flink/Spark → Impala (queries reales)
```

---

## 🚀 CDP EN POSADAS ENERGÉTICO (Caso Real)

### **Escenario:**
- Empresa de energía con 50.000 medidores
- Datos llegan cada 15 minutos
- Necesitan: detección de anomalías, predicción de demanda

### **Arquitectura CDP:**
```
┌─────────────────────────────────────────────────┐
│         MEDIDORES INTELIGENTES                  │
│         (50.000 dispositivos)                   │
└────────────────┬────────────────────────────────┘
                 │
        ┌────────▼─────────┐
        │  KAFKA (Ingesta) │ ← Recibe datos cada 15 min
        └────────┬─────────┘
                 │
        ┌────────▼──────────────┐
        │ SPARK (Transformación)│ ← Limpia, enriquece
        │ • Deduplicación      │
        │ • Validación         │
        │ • Agregación por zona│
        └────────┬──────────────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
    ▼            ▼            ▼
 Data Lake  Warehouse    ML Models
    │            │            │
    │            │            ▼
    │            │      Predicción demanda
    │            │      Detección anomalías
    │            │
    └────┬───────┴─────────┘
         │
         ▼
    Dashboard (Hue/Tableau)
    • Consumo en tiempo real
    • Alertas de anomalías
```

---

## 🎯 PUNTOS CLAVE PARA ENTREVISTA

1. **CDP es modular:** Usas solo los componentes que necesitas
2. **Spark es el corazón:** 80% de transformaciones van aquí
3. **Airflow orquesta:** Controla el flujo y scheduling
4. **Ranger protege:** Control de acceso granular
5. **Atlas governa:** Sabe qué datos existen y de dónde vienen

---

## 📚 Preguntas para hacerte a ti mismo

- ¿Cuál es la diferencia entre Data Hub y Data Warehouse en CDP?
- ¿Por qué usar Kafka en lugar de ingestar directamente a HDFS?
- ¿Qué rol juega Airflow en una arquitectura CDP?
- ¿Cómo se asegura que solo ciertos usuarios vean datos sensibles?
- ¿Qué es Delta Lake y por qué es importante?

**Ver respuestas en: FAQ_TECNICO.md**

---

## 🔗 RECURSOS PARA PROFUNDIZAR

- **Documentación oficial:** docs.cloudera.com/cdp
- **Arquitectura referencia:** Cloudera Reference Architecture
- **YouTube:** Cloudera Engineering channel

