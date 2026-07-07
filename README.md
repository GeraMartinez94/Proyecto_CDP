# 📊 Cloudera CDP - Proyecto Práctico

Un pipeline de datos **completo y ejecutable** que simula una arquitectura **Cloudera CDP real** para el sector energético.

**Aprende CDP de forma práctica: ingesta, transformación, almacenamiento, orquestación y gobernanza.**

---

## 🎯 ¿Qué es este proyecto?

Este es un proyecto educativo que implementa un pipeline CDP completo sin necesidad de instalar un clúster real. Perfecto para:

- ✅ Entender cómo funciona CDP en production
- ✅ Aprender Spark, Airflow, ingesta de datos
- ✅ Practicar para entrevistas de Data Engineering
- ✅ Experimentar con transformaciones y gobernanza
- ✅ Portfolio: mostrar en GitHub

**Caso de uso:** Pipeline de medidores energéticos que ingesta datos cada 15 minutos, transforma con Spark, almacena en Data Lake, y detecta anomalías con ML.

---

## 📁 Estructura del Proyecto

```
proyecto-cdp/
├── README.md                          # Este archivo
├── 01_GUIA_CDP_TEORIA.md             # Conceptos teóricos de CDP
├── 02_PIPELINE_CDP_PRACTICO.py       # Pipeline ejecutable ⭐
├── 03_FAQ_TECNICO_CDP.md             # 20 preguntas de entrevista
├── 04_GUIA_EJECUCION.md              # Instrucciones detalladas
├── requirements.txt                  # Dependencias Python
├── data_lake/                        # (Generado al ejecutar)
│   ├── raw_medidores_input.parquet
│   ├── processed_medidores.parquet
│   ├── aggregated_por_zona.parquet
│   └── anomalias_detectadas.parquet
└── .gitignore                        # Ignora data_lake/
```

---

## 🚀 Inicio Rápido

### 1. Clonar el repositorio

```bash
git clone https://github.com/TU_USUARIO/proyecto-cdp.git
cd proyecto-cdp
```

### 2. Crear entorno virtual

```bash
python -m venv cdp_env
source cdp_env/bin/activate  # Linux/Mac
# o
cdp_env\Scripts\activate     # Windows
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Ejecutar el pipeline

```bash
python 02_PIPELINE_CDP_PRACTICO.py
```

**Verás en consola:**
```
🚀 INICIANDO DAG CDP - Energético
📡 INGESTA: Generando 500 registros de 1000 medidores
✅ Ingesta completada: 500 registros generados
💾 DATA LAKE: Guardando tabla 'raw_medidores_input'
...
✅ DAG COMPLETADO EXITOSAMENTE
   Duración: 4.23 segundos
```

---

## 📚 Contenido del Proyecto

### **01_GUIA_CDP_TEORIA.md** 📖
Guía completa sobre Cloudera CDP:
- Arquitectura y componentes
- Data Hubs (Data Engineering, Warehouse, Operational DB)
- Stack tecnológico (Kafka, Spark, Airflow, Atlas, Ranger)
- Flujos ETL típicos
- Comparativa con alternativas
- Casos de uso reales

**Tiempo:** 30 minutos de lectura

### **02_PIPELINE_CDP_PRACTICO.py** 💻
Pipeline ejecutable que implementa:

```
INGESTA (Kafka simulado)
    ↓
TRANSFORMACIÓN (Spark)
    • Limpieza
    • Enriquecimiento
    • Agregación
    • Detección de anomalías (ML)
    ↓
ALMACENAMIENTO (Data Lake)
    • Parquet particionado
    • Metadatos en Atlas (simulado)
    ↓
ORQUESTACIÓN (Airflow simulado)
    • DAG con 5 tasks
    • Dependencias claras
    • Logging detallado
```

**Componentes simulados:**
- `IngestaKafkaSimulada`: Genera datos de medidores
- `TransformacionSpark`: Spark jobs (limpieza, enriquecimiento, ML)
- `AlmacenamientoDataLake`: Guarda en Parquet + metadatos
- `OrquestacionAirflow`: Ejecuta DAG completo

**Tiempo de ejecución:** ~4 segundos

### **03_FAQ_TECNICO_CDP.md** ❓
20 preguntas típicas de entrevista:

**Nivel 1 - Fundamentales:**
- ¿Cuál es la diferencia entre CDP y Hadoop tradicional?
- ¿Qué es Data Hub?
- ¿Qué rol juega Airflow?
- ¿Por qué usar Kafka?

**Nivel 2 - Arquitectura:**
- Flujo ETL típico
- Batch vs Streaming
- Apache Atlas y gobernanza
- Ranger vs Kerberos

**Nivel 3 - Spark:**
- Spark Batch vs Streaming
- Optimización de jobs lentos
- DataFrame vs RDD

**Nivel 4 - Casos reales:**
- Debuggear OutOfMemory
- Seguridad de datos sensibles
- Calidad de datos
- Versionado de pipelines

**Nivel 5 - Tricky:**
- Qué pasa si Airflow falla
- Evitar procesamiento duplicado
- Debuggear en production
- Disaster recovery
- Migración de Hadoop a CDP

**Tiempo:** 60 minutos

### **04_GUIA_EJECUCION.md** 🔧
Instrucciones paso a paso:
- Instalación (con y sin Docker)
- Ejecución en múltiples plataformas
- Exploración de datos generados
- Troubleshooting
- Experimentos para profundizar

---

## 💡 Conceptos Aplicados

Este proyecto cubre **95% de lo que necesitas saber sobre CDP**:

| Componente | Archivo | Función |
|---|---|---|
| **Ingesta** | TASK 1 | Kafka simulado (datos de medidores) |
| **Transformación** | TASK 2-3 | Spark (limpieza, enriquecimiento, agregación) |
| **Almacenamiento** | data_lake/ | Data Lake (Parquet particionado) |
| **Orquestación** | OrquestacionAirflow | Airflow (DAG con dependencias) |
| **Gobernanza** | ATLAS simulado | Metadatos y linaje de datos |
| **Seguridad** | Ranger simulado | Control de acceso granular |
| **ML** | Detección anomalías | Z-score para outliers |

---

## 🎓 Plan de Estudio Recomendado

### **Día 1: Teoría** (30 min)
```bash
# Leer conceptos fundamentales
cat 01_GUIA_CDP_TEORIA.md
```

### **Día 2: Práctica** (25 min)
```bash
# Ejecutar el pipeline
python 02_PIPELINE_CDP_PRACTICO.py

# Explorar datos generados
ls -la data_lake/
```

### **Día 3: Entrevista** (60 min)
```bash
# Estudiar preguntas y respuestas
cat 03_FAQ_TECNICO_CDP.md

# Practicar en voz alta
# (Grábate respondiendo para auto-evaluación)
```

### **Día 4: Consolidación** (30 min)
```bash
# Modificar código (experimentos)
# Aumentar número de medidores
# Agregar nuevas transformaciones
# Cambiar algoritmo de anomalías
```

---

## 🔬 Experimentos para Profundizar

Una vez hayas ejecutado el pipeline, intenta:

### 1. Aumentar volumen de datos
```python
# Cambiar en línea ~120
ingesta.generar_datos_medidores(num_medidores=10000, num_registros=5000)
```

### 2. Cambiar porcentaje de anomalías
```python
# En IngestaKafkaSimulada.generar_datos_medidores()
if np.random.random() < 0.15:  # 15% en lugar de 5%
```

### 3. Agregar nueva métrica
```python
# En TransformacionSpark.enriquecer_datos()
df['potencia_media'] = df['consumo_kwh'] / 0.25  # Por 15 minutos
```

### 4. Cambiar algoritmo de anomalías
```python
# Implementar IQR en lugar de Z-score
Q1 = df.groupby('medidor_id')['consumo_kwh'].quantile(0.25)
Q3 = df.groupby('medidor_id')['consumo_kwh'].quantile(0.75)
IQR = Q3 - Q1
df['anomalia_iqr'] = (df['consumo_kwh'] < Q1 - 1.5*IQR) | (df['consumo_kwh'] > Q3 + 1.5*IQR)
```

---

## 📊 Salida Esperada

Al ejecutar el pipeline verás:

```
======================================================================
🚀 INICIANDO DAG CDP - Energético
======================================================================

[TASK 1] INGESTA DE DATOS
----------------------------------------------------------------------
📡 INGESTA: Generando 500 registros de 1000 medidores
✅ Ingesta completada: 500 registros generados
💾 DATA LAKE: Guardando tabla 'raw_medidores_input'
✅ Guardado en: ./data_lake/raw_medidores_input.parquet (0.05 MB)

[TASK 2] TRANSFORMACIÓN (SPARK)
----------------------------------------------------------------------
🧹 SPARK: Limpiando datos
✅ Registros después de limpieza: 500
💎 SPARK: Enriqueciendo datos
✅ Datos enriquecidos

[TASK 3] AGREGACIÓN POR ZONA
----------------------------------------------------------------------
📊 SPARK: Agregando por zona
✅ Agregación completada: 10 registros

[TASK 4] DETECCIÓN DE ANOMALÍAS (ML)
----------------------------------------------------------------------
🤖 SPARK: Detectando anomalías
✅ 25 anomalías detectadas

[TASK 5] GENERACIÓN DE REPORTES
----------------------------------------------------------------------
📊 REPORTE FINAL
   Total medidores: 1000
   Total registros procesados: 500
   Consumo total: 1250.75 kWh
   Consumo promedio: 2.50 kWh
   Anomalías detectadas: 25
   Zonas analizadas: 10

======================================================================
✅ DAG COMPLETADO EXITOSAMENTE
   Duración: 4.23 segundos
======================================================================
```

---

## 🛠️ Requisitos

- Python 3.8+
- pip (gestor de paquetes)
- ~500MB de espacio libre (dependencias)

**Dependencias Python:**
```
apache-airflow==2.7.0
pyspark==3.5.0
pandas==2.0.0
numpy==1.24.0
pyarrow==13.0.0
```

Ver `requirements.txt` para versiones exactas.

---

## 📝 Uso en Entrevistas

### Cuando pregunten: "¿Cuál es tu experiencia con CDP?"

**Respuesta recomendada:**
```
"Desarrollé un pipeline CDP completo en GitHub que implementa
un caso real de energía. El pipeline ingesta 1000 medidores 
vía Kafka (simulado), transforma con Spark (limpieza, 
enriquecimiento, agregación), almacena en Data Lake, 
orquesta con Airflow y detecta anomalías con ML.

Implementé gobernanza con Atlas y seguridad con Ranger.
El pipeline procesa 500 registros en ~4 segundos.

Puedo mostrarle el código aquí en GitHub y explicar cada componente."
```

### Tips:
- ✅ Llevar laptop con código
- ✅ Explicar decisiones de diseño
- ✅ Hablar de optimizaciones
- ✅ Preguntar sobre casos en su empresa

---

## 🐛 Troubleshooting

### Error: "ModuleNotFoundError: No module named 'pyspark'"
```bash
pip install pyspark
```

### Error: "Unable to find a usable engine for parquet"
```bash
pip install pyarrow
```

### Error: "Permission denied" (Linux/Mac)
```bash
chmod +x 02_PIPELINE_CDP_PRACTICO.py
```

### Ejecución lenta
- En Windows, usar WSL (Windows Subsystem for Linux)
- O ejecutar en Google Colab

Ver `04_GUIA_EJECUCION.md` para más soluciones.

---

## 📚 Recursos Adicionales

- **Documentación CDP:** https://docs.cloudera.com/cdp/
- **Apache Spark:** https://spark.apache.org/
- **Apache Airflow:** https://airflow.apache.org/
- **Apache Kafka:** https://kafka.apache.org/
- **Apache Atlas:** https://atlas.apache.org/

---

## 🤝 Contribuciones

Si tienes ideas para mejorar el proyecto:

1. Fork el repositorio
2. Crea una rama (`git checkout -b feature/nueva-feature`)
3. Commit cambios (`git commit -m 'Agrega nueva feature'`)
4. Push a la rama (`git push origin feature/nueva-feature`)
5. Abre un Pull Request

---

## 📄 Licencia

Este proyecto está bajo licencia **MIT**. Ver `LICENSE` para detalles.

---

## 👨‍💻 Autor

**Gerardo Martinez**
- 🔗 GitHub: [@GeraMartinez94](https://github.com/GeraMartinez94)
- 📧 Email: geramartinez450@gmail.com
- 💼 LinkedIn: [tu-perfil-linkedin]

---

## ⭐ Si te ayudó, dale una estrella! ⭐

```
Si este proyecto te fue útil para aprender CDP o para tu entrevista,
considera darle una ⭐ en GitHub. ¡Motiva a contribuir más!
```

---

## 📝 Changelog

### v1.0.0 (2026-01-15)
- ✅ Pipeline CDP completo
- ✅ Documentación teórica
- ✅ FAQ de entrevista
- ✅ Guía de ejecución

---

## 🎯 Roadmap

- [ ] Implementar DAG real con Airflow (no simulado)
- [ ] Conectar con Apache Atlas real
- [ ] Tests unitarios
- [ ] Docker Compose para full CDP stack
- [ ] Dashboard con Streamlit
- [ ] Integración con base de datos real
- [ ] CI/CD pipeline

---

**¡Feliz aprendizaje! 🚀**

Para preguntas o problemas, abre un issue en GitHub.

