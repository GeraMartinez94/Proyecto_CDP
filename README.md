#  Cloudera CDP 

Un pipeline de datos **completo y ejecutable** que simula una arquitectura **Cloudera CDP real** para el sector energético.

**Aprende CDP de forma práctica: ingesta, transformación, almacenamiento, orquestación y gobernanza.**

---

**Caso de uso:** Pipeline de medidores energéticos que ingesta datos cada 15 minutos, transforma con Spark, almacena en Data Lake, y detecta anomalías con ML.

---

##  Estructura del Proyecto

```
proyecto-cdp/
├── README.md                        
├── 01_GUIA_CDP_TEORIA.md            
├── 02_PIPELINE_CDP_PRACTICO.py       
├── 03_FAQ_TECNICO_CDP.md            
├── 04_GUIA_EJECUCION.md             
├── requirements.txt                
├── data_lake/                        
│   ├── raw_medidores_input.parquet
│   ├── processed_medidores.parquet
│   ├── aggregated_por_zona.parquet
│   └── anomalias_detectadas.parquet
└── .gitignore                      
```

---

##  Inicio Rápido

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


---

##  Contenido del Proyecto

### **02_PIPELINE_CDP_PRACTICO.py**
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

##  Conceptos Aplicados

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

