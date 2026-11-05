# Checkpoint Semana 3 – RapidGo Customer Experience Analytics

### 1. ¿Qué quedó funcionando esta semana?

Durante la tercera semana se incorporó Apache Airflow como herramienta de orquestación del pipeline analítico, mediante el DAG `rapidgo_customer_experience_pipeline`, compuesto por cinco tareas: `check_sources`, `validate_raw`, `aggregate_kpis`, `validate_curated` y `generate_report`, organizadas secuencialmente para controlar la disponibilidad de las fuentes, validar los datos, calcular indicadores y generar reportes.

También se configuró el servicio `init-permissions` en Docker Compose para preparar automáticamente los directorios compartidos del Data Lake y los checkpoints, solucionando los conflictos de permisos entre los contenedores de Spark y Airflow. Adicionalmente, se preparó la ampliación del workflow `CI.yml` para incorporar verificaciones de importación y estructura del DAG.
