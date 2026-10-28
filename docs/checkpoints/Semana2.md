# Checkpoint Semana 2

## 1. Ejecución del pipeline: procesamiento, métricas y persistencia

Durante la segunda semana se avanzó en la implementación del flujo de procesamiento de eventos de RapidGo, integrando un productor desarrollado en Python, Kafka, PySpark Structured Streaming y el almacenamiento de resultados en un Data Lake. El productor genera eventos sintéticos de retroalimentación que contienen el identificador del pedido (`order_id`), una calificación entre 1 y 5 (`rating`), un motivo (`motivo`) y una marca temporal en milisegundos (`timestamp_ms`); estos eventos se envían al tópico `rapidgo.customer_feedback` utilizando `order_id` como clave del mensaje, de acuerdo con la decisión arquitectónica ADR-001, y se genera un nuevo evento aproximadamente cada dos segundos.

**Procesamiento y frecuencia de ejecución.** El flujo de Spark se configuró mediante dos consultas de Structured Streaming, la primera procesa y almacena los eventos en micro-batches con un intervalo de 5 segundos, mientras que la segunda calcula y actualiza las métricas cada 10 segundos. Durante las pruebas se reportó un valor de `processedRowsPerSecond: 0.2`, correspondiente a una ejecución específica del sistema; este dato sirve como referencia del comportamiento observado durante las pruebas locales, aunque no permite establecer por sí solo un rendimiento estable ni una latencia inferior a un segundo.

**Clasificación y generación de alertas.** El procesamiento incorpora reglas para clasificar los eventos según su calificación y motivo. Las calificaciones iguales o inferiores a 2 se clasifican como `ALERTA_CRITICA`, mientras que las calificaciones de 3 generan una alerta crítica cuando el motivo corresponde a `demora`, `daño` o `perdida`; los demás eventos con calificación de 3 se clasifican como `ATENCION`, y las calificaciones de 4 o 5 se clasifican como `NORMAL`. Adicionalmente, se incorpora la columna `causa_alerta`, que identifica las categorías `CALIFICACION_BAJA`, `MOTIVO_GRAVE` y `SIN_ALERTA`, facilitando la interpretación de los registros que requieren atención.

**Cálculo de métricas y almacenamiento.** Las métricas se calculan mediante ventanas temporales de 20 segundos, agrupadas por motivo, con un *watermark* de 30 segundos para gestionar la llegada de eventos tardíos. Para cada grupo se obtiene el total de registros mediante `total_quejas` y la cantidad de eventos clasificados como `ALERTA_CRITICA` mediante `criticas`. El indicador `total_quejas` contabiliza todos los eventos agrupados por motivo, incluidos los registros con motivos como `excelente` y `normal`, por lo que representa el total de eventos y no exclusivamente las quejas.

Los eventos procesados se almacenan en formato Parquet en `datalake/eventos_procesados`, mientras que los registros cuyo nivel de alerta es diferente de `NORMAL` se almacenan en `datalake/alertas`. Las escrituras se realizan en modo `append`, y las dos consultas utilizan ubicaciones de checkpoint independientes para registrar su progreso. El formato Parquet facilita el almacenamiento y la consulta posterior de los datos, aunque el script no define explícitamente una estrategia de particionamiento ni una configuración específica de compresión.

## 2. Decisiones técnicas y arquitectura

**Integración del productor con Kafka.** Se mantuvo el tópico `rapidgo.customer_feedback` como punto de intercambio de eventos entre el productor y el consumidor de Spark. El productor utiliza `localhost:9092`, mientras que el script de procesamiento utiliza `kafka:19092`, direcciones previstas para el acceso desde el equipo anfitrión y desde la red interna de Docker, respectivamente; su funcionamiento depende de que los puertos y los *listeners* estén configurados correctamente en el servicio Kafka.

El productor genera identificadores UUID para los pedidos y selecciona aleatoriamente las calificaciones y los motivos, entre ellos `demora`, `daño`, `perdida`, `excelente`, `normal` y `fraude`. Esta estrategia permite simular diferentes situaciones de experiencia del cliente sin utilizar información real, proporcionando datos para comprobar la recepción de eventos, la clasificación de alertas y el cálculo de métricas.

**Ejecución dentro de Docker.** Se priorizó la ejecución de PySpark dentro del entorno de contenedores mediante el clúster de Spark, en lugar de ejecutar el procesamiento directamente desde el entorno Python de Windows. Esta decisión reduce las diferencias entre el entorno de desarrollo y el de ejecución, además de evitar dependencias y configuraciones específicas de Hadoop que pueden aparecer al ejecutar PySpark directamente en Windows.

**Persistencia del Data Lake y los checkpoints.** Se utilizaron las rutas `/opt/spark/work-dir/datalake` y `/opt/spark/work-dir/docs/checkpoints` para almacenar los resultados y los metadatos de recuperación de las consultas. Para conservar los archivos después de detener los contenedores, se contemplaron montajes de volúmenes Docker que enlazan los directorios locales `./datalake` y `./docs` con las rutas correspondientes dentro del contenedor.

Los checkpoints permiten registrar el progreso de las consultas y facilitan su recuperación después de una interrupción, siempre que se conserve el estado correspondiente y se mantenga la compatibilidad de la consulta. No obstante, esta configuración no demuestra por sí sola una garantía *exactly-once* de extremo a extremo, especialmente porque el procesamiento utiliza `foreachBatch` y escrituras Parquet en modo `append`.

**Integración continua con GitHub Actions.** También se incorporó el archivo `CI.yml` para automatizar validaciones del repositorio mediante GitHub Actions. El flujo se ejecuta cuando se realiza un `push` hacia las ramas `main` o `preproduccion`, cuando se abre o actualiza un Pull Request dirigido a estas ramas y cuando se inicia manualmente mediante `workflow_dispatch`.

El trabajo denominado `validar` se ejecuta en un entorno `ubuntu-latest` e incluye seis etapas principales: descarga del repositorio mediante `actions/checkout@v4`, configuración de Python 3.12 mediante `actions/setup-python@v5`, instalación de las dependencias declaradas en `requirements.txt`, validación de sintaxis mediante `python -m compileall producer spark test`, ejecución de las pruebas automatizadas con `pytest -v` y validación de la configuración de Docker Compose mediante `docker compose config --quiet`.

Esta integración continua establece una revisión automática de aspectos básicos del proyecto antes de integrar cambios en las ramas principales. La compilación comprueba la sintaxis de los archivos Python incluidos en las rutas indicadas, las pruebas permiten verificar los comportamientos cubiertos por la suite automatizada y la validación de Docker Compose comprueba que la configuración pueda interpretarse correctamente. Sin embargo, estas verificaciones no sustituyen una prueba de integración completa con Kafka y Spark en ejecución, ni garantizan por sí solas que el pipeline procese correctamente los eventos en tiempo real.

## 3. Problemas encontrados y soluciones aplicadas

**Problema 1. Incompatibilidad de versiones entre Spark y el conector de Kafka.**

Durante la inicialización de la lectura del tópico se presentó la excepción `java.lang.NoSuchMethodError: 'void org.apache.spark.sql.execution.streaming.SerializedOffset.<init>(java.lang.String)'`. El problema se relacionó con una incompatibilidad entre las versiones de las dependencias utilizadas para integrar Kafka con Spark, dado que el clúster ejecutaba Spark 4.2.0 con Scala 2.13, mientras que inicialmente se había utilizado el conector `spark-sql-kafka-0-10_2.12:3.5.0`, compilado para Scala 2.12 y otra versión de Spark.

Como solución, se ajustó la dependencia para utilizar `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`, de acuerdo con las versiones del clúster. También se eliminaron los checkpoints anteriores para permitir que Structured Streaming reconstruyera los metadatos desde cero; esta operación implica perder el progreso previamente registrado, por lo que debe realizarse únicamente cuando se acepta reiniciar la consulta.

**Problema 2. Fallos en la descarga de dependencias Maven.**

Durante la resolución de dependencias se reportaron errores como `SSLHandshakeException: certificate_unknown` y `Content Length mismatch`, relacionados con descargas incompletas o fallidas de archivos JAR. Entre las posibles causas identificadas se encontraba la interferencia de la inspección HTTPS de la red local y la presencia de artefactos incompletos en la caché de Ivy.

Para abordar el problema, se eliminó la caché temporal `/tmp/.ivy2` dentro del contenedor de Spark y se estableció explícitamente esa ubicación mediante `--conf spark.jars.ivy=/tmp/.ivy2` en el comando de ejecución. Estas medidas buscaron evitar la reutilización de archivos dañados y facilitar la resolución de dependencias en el entorno del contenedor.

**Problema 3. Configuración de las conexiones de Kafka.**

La configuración del productor utiliza `localhost:9092`, mientras que el consumidor de Spark se conecta a `kafka:19092`. Para que ambos componentes puedan comunicarse con el mismo broker, Kafka debe permitir el acceso desde el host y desde la red interna de Docker, mediante los puertos y los *listeners* correspondientes. Por esta razón, la verificación de la conectividad debe considerar la configuración del servicio en `docker-compose.yml`, el envío de mensajes desde el productor y la recepción efectiva de los eventos por parte de Spark.

## 4. Validación automatizada y seguimiento del desarrollo

La incorporación de GitHub Actions permitió establecer una base para la validación continua del código y de la configuración del proyecto. El flujo `CI RapidGo` automatiza la compilación sintáctica de los archivos Python, la ejecución de las pruebas definidas en el repositorio y la comprobación de la configuración de Docker Compose, reduciendo la necesidad de realizar estas verificaciones exclusivamente de forma manual.

La ejecución del flujo depende de que las dependencias puedan instalarse correctamente desde `requirements.txt` y de que las pruebas automatizadas finalicen sin errores. Por tanto, la existencia del archivo `CI.yml` confirma la definición de estas comprobaciones, pero su funcionamiento efectivo debe respaldarse con ejecuciones satisfactorias del workflow en GitHub Actions.

Como evidencias de la semana se consideran el productor de eventos, el script de procesamiento con Spark, la configuración de Docker Compose, el archivo de integración continua, los resultados de las pruebas automatizadas y los registros de ejecución del pipeline. También son relevantes los archivos Parquet generados, los metadatos de los checkpoints y las salidas de las métricas, siempre que se hayan obtenido durante las pruebas realizadas.

En conjunto, durante la semana 2 se avanzó en la integración de la generación de eventos sintéticos con Kafka y PySpark, se incorporaron reglas para clasificar alertas y calcular métricas temporales, se establecieron mecanismos de persistencia y recuperación y se añadió una validación automatizada del repositorio mediante GitHub Actions. Estos componentes constituyen una base para continuar con las pruebas de integración, la verificación del comportamiento del pipeline y la mejora de los indicadores de experiencia del cliente.
