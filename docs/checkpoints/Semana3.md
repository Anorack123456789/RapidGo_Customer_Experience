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

### 5. Issues, Pull Requests y evidencias relacionadas

- **DAG:** `airflow/dags/rapidgo_pipeline.py`.
- **Configuración de servicios:** `docker-compose.yml`.
- **Configuración de Airflow:** `airflow/Dockerfile`.
- **Integración continua:** `.github/workflows/CI.yml`.
- **Dependencias:** `requirements.txt`.
- **Datos procesados:** `datalake/processed/`.
- **Resultados agregados:** `datalake/curated/`.
- **Reportes:** `datalake/reports/`.
- **Permisos:** registros del contenedor `rapidgo-init-permissions`.
- **Orquestación:** registros de ejecución del DAG en la interfaz de Airflow.

### 6. Fallo controlado y recuperación

Para evaluar la resiliencia del pipeline y la gestión de dependencias, se simuló un error de calidad de datos introduciendo un campo inexistente (`columna_falsa_para_fallo`) en la lista de columnas requeridas del DAG.

* **Tarea que falló:** `validate_raw`
* **Mensaje real del log:** `4 [2026-11-05 16:12:44] ERROR - Task failed with exception
▼ValueError: Columnas faltantes en /opt/airflow/datalake/processed/part-00000-59f2f946-a882-4fbb-9e34-e978470609c2-c000.snappy.parquet: {'columna_falsa_para_fallo'}`
* **Tareas afectadas:** Al fallar la validación, Airflow detuvo el flujo e impidió la ejecución de las tareas dependientes quedaron en estado `upstream_failed`: `aggregate_kpis`, `validate_curated` y `generate_report`.
* **Cambio que resolvió el problema:** Se corrigió el código eliminando la columna falsa del esquema esperado.
* **Recuperación:** Se utilizó la funcionalidad **Clear** de Airflow sobre la tarea `validate_raw`. El orquestador reanudó el flujo desde ese punto exacto utilizando el código corregido, ejecutando exitosamente el resto del pipeline sin necesidad de reiniciar la tarea `check_sources`.
* **Evidencia:** Todo el proceso quedo documentado en la rama `feature/Error_controlado` y no se realizo pull request para no implementar codigo erroneo en la rama principal.

**Estado de la semana:** Implementación realizada y validaciones parciales satisfactorias. Quedan pendientes la evidencia de una ejecución completa exitosa del DAG, una prueba de fallo controlado con su recuperación y la confirmación de una ejecución satisfactoria del workflow actualizado de GitHub Actions.