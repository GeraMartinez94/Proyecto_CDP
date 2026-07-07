"""
PIPELINE CDP PRÁCTICO - SIMULACIÓN LOCAL
=========================================

Este script simula un pipeline CDP real del sector energético.
Usa Airflow + Spark (sin Docker, solo Python).

FLUJO:
1. INGESTA: Simula datos de 1000 medidores inteligentes
2. TRANSFORMACIÓN: Limpia, valida y agrega con Spark
3. ALMACENAMIENTO: Guarda en "Data Lake" (archivos locales)
4. ANÁLISIS: Detecta anomalías
5. ORQUESTACIÓN: Todo controlado por Airflow

INSTALACIÓN:
pip install apache-airflow pyspark pandas numpy

EJECUCIÓN:
python 02_PIPELINE_CDP_PRACTICO.py
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# PASO 1: SIMULAR INGESTA (como si fuera Kafka)
# ============================================================================

class IngestaKafkaSimulada:
    """Simula ingesta de datos de medidores (como Kafka lo haría)"""
    
    @staticmethod
    def generar_datos_medidores(num_medidores=1000, num_registros=500):
        """
        Simula datos de medidores inteligentes.
        En production, estos vendrían de Kafka en tiempo real.
        """
        logger.info(f"📡 INGESTA: Generando {num_registros} registros de {num_medidores} medidores")
        
        datos = []
        timestamp = datetime.now() - timedelta(hours=1)
        
        for _ in range(num_registros):
            medidor_id = np.random.randint(1, num_medidores + 1)
            
            # Datos realistas: consumo en kWh
            consumo_base = np.random.normal(2.5, 0.8)  # Media 2.5, std 0.8
            
            # Anomalías (5% de los datos)
            if np.random.random() < 0.05:
                consumo = abs(consumo_base * np.random.uniform(3, 8))  # Anomalía
                anomalia = True
            else:
                consumo = abs(consumo_base)
                anomalia = False
            
            dato = {
                'medidor_id': medidor_id,
                'timestamp': timestamp.isoformat(),
                'consumo_kwh': round(consumo, 2),
                'zona': f"Zona_{medidor_id % 10}",  # 10 zonas distribuidas
                'temperatura_ambiente': round(np.random.normal(25, 5), 1),
                'es_anomalia': anomalia,
                'estado': 'OK' if consumo < 5 else 'WARNING'
            }
            datos.append(dato)
            timestamp += timedelta(minutes=15)
        
        logger.info(f"✅ Ingesta completada: {len(datos)} registros generados")
        return pd.DataFrame(datos)

# ============================================================================
# PASO 2: TRANSFORMACIÓN (SPARK - simulado con Pandas)
# ============================================================================

class TransformacionSpark:
    """Simula transformaciones Spark en CDP"""
    
    @staticmethod
    def limpiar_datos(df):
        """
        TRANSFORMATION 1: Limpieza de datos
        - Remover duplicados
        - Validar tipos
        - Llenar valores nulos
        """
        logger.info("🧹 SPARK: Limpiando datos")
        
        # Remover duplicados
        df_limpio = df.drop_duplicates(subset=['medidor_id', 'timestamp'])
        
        # Validar consumo positivo
        df_limpio = df_limpio[df_limpio['consumo_kwh'] > 0]
        
        # Convertir timestamp a datetime
        df_limpio['timestamp'] = pd.to_datetime(df_limpio['timestamp'])
        
        logger.info(f"✅ Registros después de limpieza: {len(df_limpio)}")
        return df_limpio
    
    @staticmethod
    def enriquecer_datos(df):
        """
        TRANSFORMATION 2: Enriquecimiento
        - Agregar características
        - Calcular consumo por hora
        - Categorizar consumo
        """
        logger.info("💎 SPARK: Enriqueciendo datos")
        
        df['hora'] = df['timestamp'].dt.hour
        df['dia_semana'] = df['timestamp'].dt.day_name()
        df['fecha'] = df['timestamp'].dt.date
        
        # Categorizar consumo
        df['categoria_consumo'] = pd.cut(
            df['consumo_kwh'],
            bins=[0, 1.5, 3, 5, float('inf')],
            labels=['Bajo', 'Normal', 'Alto', 'Crítico']
        )
        
        logger.info(f"✅ Datos enriquecidos")
        return df
    
    @staticmethod
    def agregar_por_zona(df):
        """
        TRANSFORMATION 3: Agregación
        Típico en CDP para dashboards y BI
        """
        logger.info("📊 SPARK: Agregando por zona")
        
        agregacion = df.groupby(['fecha', 'zona']).agg({
            'consumo_kwh': ['sum', 'mean', 'max'],
            'medidor_id': 'count',
            'es_anomalia': 'sum'
        }).reset_index()
        
        agregacion.columns = ['fecha', 'zona', 'consumo_total', 'consumo_promedio', 
                              'consumo_maximo', 'num_medidores', 'num_anomalias']
        
        logger.info(f"✅ Agregación completada: {len(agregacion)} registros")
        return agregacion
    
    @staticmethod
    def detectar_anomalias(df):
        """
        TRANSFORMATION 4: Machine Learning (Detección de anomalías)
        Usa Z-score para identificar outliers
        """
        logger.info("🤖 SPARK: Detectando anomalías")
        
        # Calcular Z-score por medidor
        df['z_score'] = df.groupby('medidor_id')['consumo_kwh'].transform(
            lambda x: (x - x.mean()) / (x.std() + 0.001)
        )
        
        # Anomalía si Z-score > 2.5
        df['anomalia_detectada'] = df['z_score'].abs() > 2.5
        
        num_anomalias = df['anomalia_detectada'].sum()
        logger.info(f"✅ {num_anomalias} anomalías detectadas")
        
        return df

# ============================================================================
# PASO 3: ALMACENAMIENTO (DATA LAKE)
# ============================================================================

class AlmacenamientoDataLake:
    """Simula almacenamiento en Data Lake (HDFS en production)"""
    
    @staticmethod
    def guardar_en_data_lake(df, nombre_tabla, formato='parquet'):
        """
        Simula guardado en Data Lake.
        En production: HDFS/Delta Lake
        """
        logger.info(f"💾 DATA LAKE: Guardando tabla '{nombre_tabla}'")
        
        # Crear directorio data_lake
        data_lake_dir = Path('./data_lake')
        data_lake_dir.mkdir(exist_ok=True)
        
        # Guardar archivo
        if formato == 'parquet':
            archivo = data_lake_dir / f"{nombre_tabla}.parquet"
            df.to_parquet(archivo, index=False)
        else:
            archivo = data_lake_dir / f"{nombre_tabla}.csv"
            df.to_csv(archivo, index=False)
        
        tamaño_mb = archivo.stat().st_size / (1024 * 1024)
        logger.info(f"✅ Guardado en: {archivo} ({tamaño_mb:.2f} MB)")
        
        # Registrar metadatos en "Atlas" (simulado)
        AlmacenamientoDataLake.registrar_en_atlas(nombre_tabla, len(df), tamaño_mb)
        
        return str(archivo)
    
    @staticmethod
    def registrar_en_atlas(tabla, num_registros, tamaño_mb):
        """Simula registro en Apache Atlas (gobernanza)"""
        logger.info(f"📋 ATLAS: Registrando metadatos de '{tabla}'")
        logger.info(f"   - Registros: {num_registros}")
        logger.info(f"   - Tamaño: {tamaño_mb:.2f} MB")
        logger.info(f"   - Timestamp: {datetime.now().isoformat()}")

# ============================================================================
# PASO 4: ORQUESTACIÓN (AIRFLOW)
# ============================================================================

class OrquestacionAirflow:
    """Simula orquestación Airflow"""
    
    @staticmethod
    def ejecutar_dag_cdp():
        """
        Ejecuta el DAG (Directed Acyclic Graph) completo.
        En production: Airflow lo hace con scheduler.
        """
        logger.info("="*70)
        logger.info("🚀 INICIANDO DAG CDP - Energético")
        logger.info("="*70)
        
        inicio = datetime.now()
        
        # Task 1: Ingesta
        logger.info("\n[TASK 1] INGESTA DE DATOS")
        logger.info("-" * 70)
        ingesta = IngestaKafkaSimulada()
        df_raw = ingesta.generar_datos_medidores(num_medidores=1000, num_registros=500)
        AlmacenamientoDataLake.guardar_en_data_lake(df_raw, 'raw_medidores_input', 'parquet')
        
        # Task 2: Transformación
        logger.info("\n[TASK 2] TRANSFORMACIÓN (SPARK)")
        logger.info("-" * 70)
        transformacion = TransformacionSpark()
        df_limpio = transformacion.limpiar_datos(df_raw)
        df_enriquecido = transformacion.enriquecer_datos(df_limpio)
        AlmacenamientoDataLake.guardar_en_data_lake(df_enriquecido, 'processed_medidores', 'parquet')
        
        # Task 3: Agregación
        logger.info("\n[TASK 3] AGREGACIÓN POR ZONA")
        logger.info("-" * 70)
        df_agregado = transformacion.agregar_por_zona(df_enriquecido)
        AlmacenamientoDataLake.guardar_en_data_lake(df_agregado, 'aggregated_por_zona', 'parquet')
        
        # Task 4: Detección de Anomalías
        logger.info("\n[TASK 4] DETECCIÓN DE ANOMALÍAS (ML)")
        logger.info("-" * 70)
        df_con_anomalias = transformacion.detectar_anomalias(df_enriquecido)
        AlmacenamientoDataLake.guardar_en_data_lake(df_con_anomalias, 'anomalias_detectadas', 'parquet')
        
        # Task 5: Resumen y Métricas
        logger.info("\n[TASK 5] GENERACIÓN DE REPORTES")
        logger.info("-" * 70)
        OrquestacionAirflow.generar_reporte(df_enriquecido, df_agregado, df_con_anomalias)
        
        # Task 6: Completado
        duracion = (datetime.now() - inicio).total_seconds()
        logger.info("\n" + "="*70)
        logger.info(f"✅ DAG COMPLETADO EXITOSAMENTE")
        logger.info(f"   Duración: {duracion:.2f} segundos")
        logger.info("="*70)
    
    @staticmethod
    def generar_reporte(df_raw, df_agregado, df_anomalias):
        """Genera reporte de ejecutión"""
        logger.info("\n📊 REPORTE FINAL")
        logger.info(f"   Total medidores: {df_raw['medidor_id'].nunique()}")
        logger.info(f"   Total registros procesados: {len(df_raw)}")
        logger.info(f"   Consumo total: {df_raw['consumo_kwh'].sum():.2f} kWh")
        logger.info(f"   Consumo promedio: {df_raw['consumo_kwh'].mean():.2f} kWh")
        logger.info(f"   Anomalías detectadas: {df_anomalias['anomalia_detectada'].sum()}")
        logger.info(f"   Zonas analizadas: {df_agregado['zona'].nunique()}")

# ============================================================================
# ANÁLISIS Y VISUALIZACIÓN
# ============================================================================

def mostrar_estadisticas():
    """Muestra estadísticas del pipeline ejecutado"""
    logger.info("\n" + "="*70)
    logger.info("📈 ESTADÍSTICAS DEL PIPELINE")
    logger.info("="*70)
    
    # Leer los archivos generados
    data_lake_dir = Path('./data_lake')
    if data_lake_dir.exists():
        archivos = list(data_lake_dir.glob('*.parquet'))
        logger.info(f"\n✅ Archivos generados en Data Lake: {len(archivos)}")
        
        for archivo in archivos:
            df = pd.read_parquet(archivo)
            logger.info(f"\n📁 {archivo.name}")
            logger.info(f"   Filas: {len(df)}")
            logger.info(f"   Columnas: {len(df.columns)}")
            logger.info(f"   Columnas: {', '.join(df.columns[:5])}")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Ejecutar pipeline completo
    OrquestacionAirflow.ejecutar_dag_cdp()
    
    # Mostrar estadísticas
    mostrar_estadisticas()
    
    print("\n" + "="*70)
    print("💡 CONCEPTOS APLICADOS EN ESTE PIPELINE:")
    print("="*70)
    print("""
    1. INGESTA (Kafka simulado)
       └─ Genera datos como si vinieran de medidores reales
    
    2. TRANSFORMACIÓN (Spark)
       ├─ Limpieza: Remover duplicados, validar tipos
       ├─ Enriquecimiento: Agregar features útiles
       ├─ Agregación: Sumarizar por zona
       └─ ML: Detección de anomalías con Z-score
    
    3. ALMACENAMIENTO (Data Lake)
       └─ Guardar en Parquet (formato eficiente)
       └─ Registrar metadatos en Atlas
    
    4. ORQUESTACIÓN (Airflow)
       └─ DAG: Tasks secuenciales con dependencias
       └─ Ejecución controlada: 1→2→3→4→5
    
    5. GOBERNANZA (Ranger + Atlas)
       ├─ Ranger: Control de acceso (simulado)
       └─ Atlas: Linaje de datos (simulado)
    """)
    print("="*70)
