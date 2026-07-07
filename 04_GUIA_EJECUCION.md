# 🚀 GUÍA DE EJECUCIÓN - Proyecto CDP Local

## **Resumen rápido**

Tienes 3 archivos listos:

1. **01_GUIA_CDP_TEORIA.md** - Teoría completa (leer primero)
2. **02_PIPELINE_CDP_PRACTICO.py** - Código ejecutable
3. **03_FAQ_TECNICO_CDP.md** - Preguntas de entrevista

---

## **PASO 1: INSTALACIÓN**

### Opción A: En tu máquina local (recomendado)

```bash
# 1. Crear un entorno virtual (recomendado)
python -m venv cdp_env
source cdp_env/bin/activate  # Linux/Mac
# o
cdp_env\Scripts\activate     # Windows

# 2. Instalar dependencias
pip install apache-airflow pyspark pandas numpy

# Nota: Primera vez puede tomar 5-10 minutos
```

### Opción B: En Google Colab (sin instalar nada)

```python
# Abrir: https://colab.research.google.com/

# Copiar en primera celda:
!pip install apache-airflow pyspark pandas numpy

# Luego cargar el archivo 02_PIPELINE_CDP_PRACTICO.py
```

---

## **PASO 2: EJECUTAR EL PIPELINE**

### Opción A: Línea de comandos
```bash
# Estando en la carpeta donde descargaste el archivo
python 02_PIPELINE_CDP_PRACTICO.py
```

### Opción B: IDE (VSCode, PyCharm)
```
1. Abrir 02_PIPELINE_CDP_PRACTICO.py
2. Click en "Run" (▶)
3. Ver output en consola
```

### Opción C: Jupyter Notebook
```python
# En notebook:
%run 02_PIPELINE_CDP_PRACTICO.py
```

---

## **PASO 3: QUÉ VERÁS EN LA SALIDA**

Cuando ejecutes, verás esto:

```
2024-01-15 10:30:45 - INFO - ======================================================================
2024-01-15 10:30:45 - INFO - 🚀 INICIANDO DAG CDP - Energético
2024-01-15 10:30:45 - INFO - ======================================================================

2024-01-15 10:30:46 - INFO - [TASK 1] INGESTA DE DATOS
2024-01-15 10:30:46 - INFO - 📡 INGESTA: Generando 500 registros de 1000 medidores
2024-01-15 10:30:46 - INFO - ✅ Ingesta completada: 500 registros generados
2024-01-15 10:30:46 - INFO - 💾 DATA LAKE: Guardando tabla 'raw_medidores_input'
2024-01-15 10:30:46 - INFO - ✅ Guardado en: ./data_lake/raw_medidores_input.parquet (0.05 MB)
2024-01-15 10:30:46 - INFO - 📋 ATLAS: Registrando metadatos de 'raw_medidores_input'

[... más tasks ...]

2024-01-15 10:30:50 - INFO - ✅ DAG COMPLETADO EXITOSAMENTE
2024-01-15 10:30:50 - INFO -    Duración: 4.23 segundos

2024-01-15 10:30:50 - INFO - 📊 REPORTE FINAL
2024-01-15 10:30:50 - INFO -    Total medidores: 1000
2024-01-15 10:30:50 - INFO -    Total registros procesados: 500
2024-01-15 10:30:50 - INFO -    Consumo total: 1250.75 kWh
2024-01-15 10:30:50 - INFO -    Consumo promedio: 2.50 kWh
2024-01-15 10:30:50 - INFO -    Anomalías detectadas: 25
2024-01-15 10:30:50 - INFO -    Zonas analizadas: 10
```

---

## **PASO 4: ARCHIVOS GENERADOS**

Se creará una carpeta `data_lake/` con archivos:

```
data_lake/
├─ raw_medidores_input.parquet      # Datos brutos (ingesta)
├─ processed_medidores.parquet      # Datos limpios (transformación)
├─ aggregated_por_zona.parquet      # Agregados (BI ready)
└─ anomalias_detectadas.parquet     # Anomalías (ML)
```

Estos archivos representan el **Data Lake** en production.

---

## **PASO 5: EXPLORAR LOS DATOS (Opcional)**

### Con Pandas (Python)
```python
import pandas as pd

# Leer datos procesados
df = pd.read_parquet('./data_lake/processed_medidores.parquet')

# Explorar
print(df.head())
print(df.describe())
print(df.info())

# Filtrar anomalías
anomalias = df[df['anomalia_detectada'] == True]
print(f"Anomalías encontradas: {len(anomalias)}")
```

### Con SQL (si instalaras DuckDB)
```bash
pip install duckdb
```

```python
import duckdb

# Query SQL sobre parquet
result = duckdb.query("""
    SELECT zona, COUNT(*) as num_registros, AVG(consumo_kwh) as promedio
    FROM './data_lake/processed_medidores.parquet'
    GROUP BY zona
""").to_df()

print(result)
```

---

## **PASO 6: CONCEPTOS APLICADOS**

Al ejecutar este pipeline, practicaste:

| Concepto | Dónde lo ves |
|----------|------------|
| **Ingesta** | TASK 1 - generación de datos (simula Kafka) |
| **Transformación** | TASK 2-3 - limpieza, enriquecimiento, agregación |
| **Almacenamiento** | Guardado en `data_lake/` (simula HDFS/Delta) |
| **Orquestación** | Secuencia de tasks (simula Airflow DAG) |
| **Gobernanza** | Registro en "Atlas" (metadatos) |
| **ML** | TASK 4 - detección de anomalías (Z-score) |

---

## **PASO 7: PREGUNTAS PARA LA ENTREVISTA**

Ahora puedes responder cosas como:

**"¿Cuál es tu experiencia con pipelines CDP?"**

Respuesta:
> "Desarrollé un pipeline completo: ingesta con Kafka simulada, transformación con Spark
> (limpieza, enriquecimiento, agregación), almacenamiento en Data Lake, orquestación con
> Airflow y detección de anomalías. Implementé gobernanza con Atlas. El pipeline procesaba
> 500 registros de 1000 medidores en ~4 segundos."

---

## **PASO 8: PROFUNDIZAR (Opcional)**

### Modificar el código:
```python
# Cambiar número de medidores
ingesta.generar_datos_medidores(num_medidores=5000)  # Más data

# Ajustar anomalías
if np.random.random() < 0.10:  # 10% anomalías en lugar de 5%

# Agregar nueva métrica
df['potencia_media'] = df['consumo_kwh'] / 0.25  # Por 15 minutos
```

### Experimentos:
1. Aumenta datos y mide performance
2. Añade nueva transformación
3. Cambia algoritmo de detección de anomalías
4. Agrega tests unitarios

---

## **SOLUCIÓN DE PROBLEMAS**

### Problema: "ModuleNotFoundError: No module named 'pyspark'"
```bash
pip install pyspark
```

### Problema: "Permission denied"
```bash
chmod +x 02_PIPELINE_CDP_PRACTICO.py
```

### Problema: "Slow en Windows"
- Usar WSL (Windows Subsystem for Linux)
- O ejecutar en Google Colab

### Problema: "No genera data_lake/"
- Verificar permisos en la carpeta
- Ejecutar desde carpeta correcta: `cd Downloads/` (o donde lo descargaste)

---

## **PRÓXIMOS PASOS PARA LA ENTREVISTA**

### ✅ Hoy:
1. ✅ Leer GUIA_CDP_TEORIA.md
2. ✅ Ejecutar PIPELINE_CDP_PRACTICO.py
3. ✅ Explorar archivos generados

### ✅ Mañana:
4. Leer FAQ_TECNICO_CDP.md
5. Practicar respuestas en voz alta
6. Modificar código (experimentos)

### ✅ Día de entrevista:
7. Llevar laptop con proyecto listo
8. Estar preparado para explicar arquitectura
9. Responder con confianza

---

## **CHEAT SHEET - COMANDOS RÁPIDOS**

```bash
# Instalar todo de una vez
pip install apache-airflow pyspark pandas numpy duckdb

# Ejecutar pipeline
python 02_PIPELINE_CDP_PRACTICO.py

# Ver archivos generados
ls -la data_lake/

# Explorar datos
python -c "import pandas as pd; df = pd.read_parquet('./data_lake/processed_medidores.parquet'); print(df.head())"

# Limpiar (remover data_lake/)
rm -rf data_lake/
```

---

## **RECURSOS ADICIONALES**

- **Documentación Spark:** https://spark.apache.org/docs/latest/
- **Airflow:** https://airflow.apache.org/
- **Cloudera:** https://www.cloudera.com/products/cdp.html
- **Parquet:** https://parquet.apache.org/

---

## **¿PREGUNTAS?**

Si algo no funciona:
1. Revisa los logs (stdout)
2. Verifica instalación: `pip list | grep -E "pyspark|pandas"`
3. Ejecuta en Google Colab (más seguro)

**¡Buena suerte en la entrevista! 🚀**

