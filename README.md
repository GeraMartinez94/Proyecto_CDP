#  CDP Data Pipeline: Solución Energética End-to-End

Arquitectura de datos **completa y ejecutable** que simula un entorno de **Cloudera Data Platform (CDP)** aplicado al sector energético. El proyecto demuestra el ciclo de vida completo de un dato, desde la ingesta en alta frecuencia hasta la gobernanza y análisis avanzado.

---

##  Objetivo del Proyecto

Implementar un pipeline de telemetría para **Smart Meters** que procesa datos cada 15 minutos, aplicando:

✅ Transformaciones distribuidas con Spark  
✅ Detección de anomalías mediante Machine Learning  
✅ Persistencia optimizada en un Data Lake  
✅ Gobernanza y linaje de datos  
✅ Orquestación de workflows con Airflow  

---

## 🏗️ Arquitectura del Sistema

El flujo de trabajo sigue las mejores prácticas de ingeniería de datos en entornos CDP:

```
┌─────────────────────────────────────────────────────────────┐
│                    PIPELINE CDP                              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  INGESTA (Kafka)    TRANSFORMACIÓN (Spark)  ALMACENAMIENTO  │
│  ────────────       ──────────────────────  ───────────────  │
│  • 1000 medidores   • Limpieza              • Data Lake      │
│  • 500 registros    • Enriquecimiento       • Parquet        │
│  • 15 min interval  • Agregación por zona   • Particionado   │
│  • JSON/Real-time   • Detección anomalías   • Atlas metadata │
│                                                               │
│              ORQUESTACIÓN (Airflow DAG)                      │
│              ├─ Task 1: Ingesta                             │
│              ├─ Task 2: Transformación                      │
│              ├─ Task 3: Agregación                          │
│              ├─ Task 4: ML (Anomalías)                      │
│              └─ Task 5: Reporte                             │
│                                                               │
│         GOBERNANZA & SEGURIDAD (Ranger + Atlas)             │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 📊 Flujo de Datos

```
MEDIDORES INTELIGENTES (1000)
        ↓
    [KAFKA] ← Ingesta simulada
        ↓
    [SPARK] ← Transformación
        │
        ├─ Limpieza
        ├─ Enriquecimiento
        ├─ Agregación
        └─ ML (Anomalías)
        ↓
  [DATA LAKE] ← Almacenamiento
        ↓
    [ATLAS] ← Gobernanza (Metadatos)
        ↓
  [RANGER] ← Seguridad (Control acceso)
```

---

## 🛠️ Stack Tecnológico

| Capa | Tecnología | Descripción |
|------|------------|-------------|
| **Motor de Cómputo** | PySpark 3.5.0 | Procesamiento distribuido de datos |
| **Orquestación** | Apache Airflow 2.7.0 | Gestión de workflows y DAGs |
| **Procesamiento** | Pandas / NumPy | Transformaciones de datos |
| **Serialización** | PyArrow | Formato Parquet eficiente |
| **Gobernanza** | Simulación Atlas | Linaje de datos y metadatos |
| **Seguridad** | Simulación Ranger | Control granular de acceso |

---

## 🚀 Inicio Rápido

### 1️⃣ Clonación y Configuración

```bash
# Clonar repositorio
git clone https://github.com/GeraMartinez94/proyecto-cdp.git
cd proyecto-cdp

# Crear entorno virtual
python -m venv cdp_env

# Activar entorno
# Linux/Mac:
source cdp_env/bin/activate
# Windows:
cdp_env\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### 2️⃣ Ejecución del Pipeline

```bash
python 02_PIPELINE_CDP_PRACTICO.py
```

**Salida esperada:**
```
2026-07-07 01:57:38 - INFO - 🚀 INICIANDO DAG CDP - Energético
2026-07-07 01:57:38 - INFO - 📡 INGESTA: Generando 500 registros de 1000 medidores
2026-07-07 01:57:39 - INFO - ✅ Ingesta completada: 500 registros generados
2026-07-07 01:57:40 - INFO - 🧹 SPARK: Limpiando datos
2026-07-07 01:57:41 - INFO - 💎 SPARK: Enriqueciendo datos
2026-07-07 01:57:42 - INFO - 📊 SPARK: Agregando por zona
2026-07-07 01:57:43 - INFO - 🤖 SPARK: Detectando anomalías
2026-07-07 01:57:44 - INFO - ✅ 25 anomalías detectadas
2026-07-07 01:57:45 - INFO - ✅ DAG COMPLETADO EXITOSAMENTE
2026-07-07 01:57:45 - INFO -    Duración: 4.23 segundos
```

### 3️⃣ Explorar Datos Generados

```bash
# Ver archivos generados
ls -la data_lake/

# Explorar con Python
python -c "import pandas as pd; df = pd.read_parquet('./data_lake/processed_medidores.parquet'); print(df.head())"
```

---

## 📂 Estructura del Proyecto

```
proyecto-cdp/
├── README.md                          # Este archivo
├── 01_GUIA_CDP_TEORIA.md             # Conceptos teóricos de CDP
├── 02_PIPELINE_CDP_PRACTICO.py       # Pipeline ejecutable 
├── 04_GUIA_EJECUCION.md              # Instrucciones detalladas
├── requirements.txt                  # Dependencias Python
├── .gitignore                        # Git ignore rules
├── LICENSE                           # MIT License
├── data_lake/                        # (Generado al ejecutar)
│   ├── raw_medidores_input.parquet
│   ├── processed_medidores.parquet
│   ├── aggregated_por_zona.parquet
│   └── anomalias_detectadas.parquet
└── .github/                          # (Opcional)
    └── workflows/
        └── tests.yml
```

---

## 📚 Documentación

| Archivo | Contenido | Tiempo |
|---------|----------|--------|
| **01_GUIA_CDP_TEORIA.md** | Conceptos, arquitectura, componentes CDP | 30 min |
| **02_PIPELINE_CDP_PRACTICO.py** | Pipeline ejecutable completo | 20 min |
| **03_FAQ_TECNICO_CDP.md** | 20 preguntas típicas de entrevista | 60 min |
| **04_GUIA_EJECUCION.md** | Instrucciones instalación y troubleshooting | 10 min |

---

## 💡 Componentes Principales

### 1. **Ingesta (Kafka Simulado)**
```python
class IngestaKafkaSimulada:
    """Genera 500 registros de 1000 medidores inteligentes"""
    - Simula flujo continuo de datos
    - Introduce anomalías (5% de los datos)
    - Datos realistas de consumo eléctrico
```

### 2. **Transformación (Spark)**
```python
class TransformacionSpark:
    - Limpieza: Remover duplicados, validar tipos
    - Enriquecimiento: Agregar features (hora, día, categoría)
    - Agregación: Agrupar por zona y periodo
    - ML: Detección de anomalías con Z-Score
```

### 3. **Almacenamiento (Data Lake)**
```python
class AlmacenamientoDataLake:
    - Guardado en Parquet (formato eficiente)
    - Particionado por fecha y zona
    - Metadatos registrados en Atlas
```

### 4. **Orquestación (Airflow)**
```python
class OrquestacionAirflow:
    - DAG con 5 tasks secuenciales
    - Dependencias claras
    - Logging detallado
    - Duración total: ~4 segundos
```

---

## 🔬 Casos de Uso Demostrados

### ✅ Ingesta en Tiempo Real
Simula Kafka ingiriendo datos de 1000 medidores cada 15 minutos

### ✅ Transformación Distribuida
PySpark limpia, enriquece y agrega datos a escala

### ✅ Machine Learning
Detección automática de anomalías usando Z-Score

### ✅ Gobernanza
Atlas registra linaje y metadatos de datos

### ✅ Seguridad
Ranger simula control de acceso granular

### ✅ Orquestación
Airflow DAG gestiona el flujo de tasks

---

## 🧪 Experimentos para Profundizar

### 1. Aumentar volumen de datos
```python
# Cambiar número de medidores
ingesta.generar_datos_medidores(num_medidores=10000, num_registros=5000)
```

### 2. Modificar porcentaje de anomalías
```python
# En IngestaKafkaSimulada.generar_datos_medidores()
if np.random.random() < 0.15:  # 15% en lugar de 5%
```

### 3. Agregar nueva métrica
```python
# En TransformacionSpark.enriquecer_datos()
df['potencia_media'] = df['consumo_kwh'] / 0.25
```

### 4. Cambiar algoritmo de detección
```python
# Implementar IQR en lugar de Z-Score
Q1 = df.groupby('medidor_id')['consumo_kwh'].quantile(0.25)
Q3 = df.groupby('medidor_id')['consumo_kwh'].quantile(0.75)
```

---

## 📋 Requisitos

- **Python:** 3.8 o superior
- **Sistema Operativo:** Linux, macOS, Windows
- **Espacio en disco:** ~500MB (incluyendo dependencias)
- **RAM:** Mínimo 2GB, recomendado 4GB+

### Dependencias Python

```
apache-airflow==2.7.0
pyspark==3.5.0
pandas==2.0.0
numpy==1.24.0
pyarrow==13.0.0
```

Ver `requirements.txt` para lista completa.

---

## 🐛 Solución de Problemas

### ❌ Error: "ModuleNotFoundError: No module named 'pyspark'"
```bash
pip install pyspark
```

### ❌ Error: "Unable to find engine for parquet"
```bash
pip install pyarrow
```

### ❌ Error: "Permission denied" (Linux/Mac)
```bash
chmod +x 02_PIPELINE_CDP_PRACTICO.py
python 02_PIPELINE_CDP_PRACTICO.py
```

### ❌ Ejecución lenta
- En Windows: Usar WSL (Windows Subsystem for Linux)
- O ejecutar en Google Colab

Ver `04_GUIA_EJECUCION.md` para más soluciones.

---


## 🚦 Roadmap Técnico

- [ ] Integración con Apache Atlas real
- [ ] Despliegue con Docker Compose
- [ ] Suite de pruebas unitarias (pytest)
- [ ] Dashboard interactivo con Streamlit
- [ ] CI/CD con GitHub Actions
- [ ] Integración con base de datos PostgreSQL
- [ ] Monitoreo con Prometheus + Grafana
- [ ] Soporte para datos streaming real

---

## 📊 Métricas del Pipeline

| Métrica | Valor |
|---------|-------|
| **Medidores procesados** | 1,000 |
| **Registros por ejecución** | 500 |
| **Duración total** | ~4 segundos |
| **Anomalías detectadas** | ~25 (5%) |
| **Zonas analizadas** | 10 |
| **Consumo promedio (kWh)** | 2.50 |

---

## 🔗 Recursos Adicionales

- **Documentación CDP:** https://docs.cloudera.com/cdp/
- **Apache Spark:** https://spark.apache.org/
- **Apache Airflow:** https://airflow.apache.org/
- **Apache Kafka:** https://kafka.apache.org/
- **Apache Atlas:** https://atlas.apache.org/

---

##  Contribuciones

Si tienes ideas para mejorar el proyecto:

1. Fork el repositorio
2. Crea una rama (`git checkout -b feature/nueva-feature`)
3. Commit cambios (`git commit -m 'Agrega nueva feature'`)
4. Push a la rama (`git push origin feature/nueva-feature`)
5. Abre un Pull Request

---

## 📄 Licencia

Este proyecto está bajo licencia **MIT**. Ver archivo `LICENSE` para detalles completos.

---

## 👨‍💻 Autor

**Gerardo Martinez**

- 🔗 **GitHub:** [@GeraMartinez94](https://github.com/GeraMartinez94)
- 📧 **Email:** geramartinez450@gmail.com
- 💼 **LinkedIn:**https://www.linkedin.com/in/gerardo-m-4a2aba231/
- 🌐 **Portfolio:** geramar94.pythonanywhere.com

---

## 📝 Changelog

### v1.0.0 (2026-01-15)
- ✅ Pipeline CDP completo y ejecutable
- ✅ Documentación teórica comprehensive
- ✅ FAQ de entrevista (20 preguntas)
- ✅ Guía de ejecución paso a paso
- ✅ Simulación de Atlas + Ranger
- ✅ Detección de anomalías con ML

---

## 📞 Soporte

¿Problemas o preguntas?

- 📌 Abre un [Issue en GitHub](https://github.com/GeraMartinez94/proyecto-cdp/issues)
- 💬 Revisa la sección FAQ en `03_FAQ_TECNICO_CDP.md`
- 📖 Lee la guía de ejecución en `04_GUIA_EJECUCION.md`

---

---

<div align="center">

Made with ❤️ by [Gerardo Martinez](https://github.com/GeraMartinez94)

Copyright © 2026 | MIT License | All Rights Reserved

</div>
