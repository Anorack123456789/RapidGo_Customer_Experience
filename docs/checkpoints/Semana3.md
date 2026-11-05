# Checkpoint Semana 3 – RapidGo Customer Experience Analytics

### 1. ¿Qué quedó funcionando esta semana?

Durante la tercera semana se incorporó Apache Airflow como herramienta de orquestación del pipeline analítico, mediante el DAG `rapidgo_customer_experience_pipeline`, compuesto por cinco tareas: `check_sources`, `validate_raw`, `aggregate_kpis`, `validate_curated` y `generate_report`, organizadas secuencialmente para controlar la disponibilidad de las fuentes, validar los datos, calcular indicadores y generar reportes.

También se configuró el servicio `init-permissions` en Docker Compose para preparar automáticamente los directorios compartidos del Data Lake y los checkpoints, solucionando los conflictos de permisos entre los contenedores de Spark y Airflow. Adicionalmente, se preparó la ampliación del workflow `CI.yml` para incorporar verificaciones de importación y estructura del DAG.

### 2. Valores o evidencias observadas durante la ejecución

- Airflow reconoce el DAG `rapidgo_customer_experience_pipeline` y no presenta errores de importación.
- Se definieron cinco tareas con sus respectivas dependencias.
- El servicio `init-permissions` finalizó correctamente con código de salida `0`, confirmando la preparación de los directorios compartidos.
- Se comprobó que Airflow puede crear y eliminar archivos dentro del directorio `datalake/curated`.
- Se verificó el acceso de escritura desde los contenedores de Spark después de ajustar los permisos.
- Se identificó que los archivos Parquet utilizan las columnas `reason` y `timestamp`, por lo que se actualizaron las validaciones que inicialmente esperaban `motivo` y `timestamp_ms`.

La ejecución completa del DAG y la recuperación ante un fallo controlado todavía requieren evidencias específicas de sus respectivos registros.

### 3. Decisiones técnicas tomadas y justificación

Se decidió incorporar Apache Airflow 3.3.2 para centralizar la ejecución y supervisión de las tareas analíticas, utilizando un DAG con cinco actividades independientes y mecanismos de comunicación mediante XCom y reintentos.

Se estableció el uso de PyArrow para validar la estructura de los archivos Parquet y comprobar condiciones básicas de calidad antes de realizar las agregaciones. También se mantuvieron directorios diferenciados para los datos procesados, los resultados agregados, los reportes y los checkpoints.

Para solucionar los problemas de acceso entre contenedores se incorporó `init-permissions`, asignando el grupo compartido `185` a los directorios del Data Lake y configurando los permisos de escritura correspondientes. Finalmente, se preparó la ampliación de GitHub Actions para comprobar la importación del DAG, la existencia de sus tareas y las dependencias definidas.

### 4. Problemas encontrados y cómo se resolvieron

Se presentaron dos dificultades principales durante la implementación. La primera correspondió a una incompatibilidad entre los nombres de las columnas esperadas por Airflow y el esquema real de los archivos Parquet; se corrigieron las columnas utilizadas en las validaciones y agregaciones.

La segunda dificultad fue un error `PermissionError` cuando Airflow intentó escribir resultados en `datalake/curated`, debido a las diferencias entre los usuarios de los contenedores. Para solucionarlo se implementó el servicio `init-permissions`, se configuró el grupo compartido y se ajustaron los permisos de los directorios. También se corrigieron las pruebas de escritura en Spark para utilizar `python3`, que es el ejecutable disponible en sus contenedores.