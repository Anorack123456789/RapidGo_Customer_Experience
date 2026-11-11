# RapidGo Customer Experience - Variante 7

**Integrantes del equipo:**

* Integrante 1: Jeison Sanchez Buitrago
* Integrante 2: Jaime Alberto Florez Paez
* Integrante 3: Andres Felipe Triana Ospina

## 1. Problema de negocio y valor de la solución

RapidGo Customer Experience Analytics es un proyecto orientado al análisis de la experiencia del cliente en una plataforma de entregas. La solución busca identificar cambios en la satisfacción, reconocer los motivos más frecuentes de insatisfacción y generar alertas ante situaciones que puedan afectar la percepción del servicio.

El deterioro de la satisfacción de los clientes puede manifestarse mediante calificaciones bajas y concentraciones de reclamos relacionados con demoras, pedidos incompletos, productos dañados o problemas de atención. El análisis de estos eventos permitirá reconocer patrones y proporcionar información útil para la toma de decisiones, aportando valor al negocio mediante la detección temprana de incidentes operativos.

## 2. Arquitectura y Tecnologías

El pipeline está diseñado bajo una arquitectura de datos en streaming y orquestación por lotes. Los eventos son generados en formato JSON y publicados en Kafka; posteriormente, Spark los valida y procesa para calcular los indicadores y detectar las condiciones de alerta; los datos se almacenan en el Data Lake y Airflow orquesta las tareas de validación, agregación y generación de reportes.

**Tecnologías utilizadas:**

* **Generador de opiniones en Python:** Simulación de opiniones y reclamos en tiempo real.
* **Apache Kafka:** Captura y transporte de eventos operacionales.
* **Apache Spark Structured Streaming:** Consumo, transformación, aplicación de reglas de negocio y persistencia de datos.
* **Data Lake Local:** Almacenamiento distribuido en formato Parquet organizado en capas (Raw, Processed, Curated).
* **Apache Airflow 3.3.2:** Orquestación, control de calidad, agregación de KPIs y generación de reportes.
* **Docker & Docker Compose:** Contenerización y despliegue local de la infraestructura.
* **GitHub Actions:** Integración continua, validación de sintaxis, pruebas automatizadas y releases.
## 3. Topics, Eventos, KPIs y Reglas principales

* **Topic:** `rapidgo.customer_feedback` (3 particiones).
* **Estructura del JSON**
```json
{
  "order_id": "uuid",
  "rating": 3,
  "motivo": "demora",
  "timestamp_ms": 1730000000000
}

```
## Indicadores KPIs:

| KPI | Unidad de medida | Ventana inicial | Propósito |
| --- | --- | --- | --- |
| Calificación media del cliente | Puntos de 1 a 5 | 1 minuto | Evaluar la satisfacción general |
| Motivos frecuentes de insatisfacción | Cantidad de opiniones negativas por motivo | 1 minuto | Identificar problemas recurrentes |
| Tendencia de satisfacción | Diferencia en puntos | Dos ventanas consecutivas de 1 minuto | Detectar cambios en la satisfacción |

## Alertas

| Nombre de la alerta | Objetivo | Condición de activación | Datos utilizados | Acción esperada |
| --- | --- | --- | --- | --- |
| Caída de satisfacción | Detectar una disminución en la satisfacción general de los clientes. | La calificación media de una ventana de 1 min es inferior a 3,5 sobre 5. | Calificaciones (`rating`) y marcas temporales (`timestamp`). | Generar una alerta que registre la calificación media obtenida y la ventana temporal. |
| Concentración de reclamos | Identificar un incremento de opiniones negativas asociadas con un mismo motivo. | Se registran 5+ opiniones con calificación <= 2, asociadas al mismo motivo, durante 1 min. | Calificaciones (`rating`), motivos (`reason`) y marcas temporales (`timestamp`). | Generar una alerta que identifique el motivo del reclamo y la cantidad de opiniones negativas. |
## 4. Prerrequisitos y Pasos para ejecutar localmente

**Prerrequisitos:**

* Docker y Docker Compose instalados.
* Python 3.12+ (para pruebas o ejecución local del productor).

**Pasos de ejecución:**

**1. Clonar el repositorio y ubicarse en la raíz del proyecto.**\
**2. Levantar la infraestructura (Kafka, Spark, Airflow, y creación de directorios):**
```bash
docker-compose up -d

```
**3. Iniciar el productor de eventos en una terminal:**
```bash
python producer/producer.py

```
**4. Iniciar el consumidor de Spark en una segunda terminal:**
```bash
docker exec -it rapidgo-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/spark/work-dir/spark/streaming.py

```
## 5. Cómo acceder a Airflow y ejecutar el DAG

**1. Acceder a la interfaz web ingresando a** `http://localhost:8083` **en el navegador.**\
**2. Obtener la contraseña generada automáticamente ejecutando:**
```bash
docker logs rapidgo-airflow | grep -i password

```
3. **Iniciar sesión con el usuario** `admin` **y la contraseña obtenida.**
4. **Localizar el DAG** `rapidgo_customer_experience_pipeline`.
5. **Activar el DAG y hacer clic en <u>Trigger DAG</u> para ejecutar el pipeline de validación y reporting.**

## 6. Cómo ejecutar pruebas y GitHub Actions

* **Pruebas Locales:** Ejecutar el framework de pruebas de Python en la raíz del proyecto mediante el comando `pytest -v`.
* **GitHub Actions:** El flujo de CI se dispara automáticamente ante eventos `push` o `pull_request`. Para ejecutarlo manualmente y cumplir con el Quality Gate, ir a la pestaña Actions en GitHub, seleccionar el workflow CI RapidGo, y hacer clic en Run workflow (`workflow_dispatch`).

## 7. Resultados o evidencias representativas

* **Persistencia:** Archivos Parquet generados correctamente en las rutas físicas locales `datalake/raw/`, `datalake/processed/` y `datalake/curated/`.
* **Reportes:** Generación automática de resúmenes en Markdown (`datalake/reports/rapidgo_report_*.md`) tras la ejecución del DAG.
* **Resiliencia:** Recuperación exitosa del pipeline documentada en un escenario de fallo controlado.

## 8. Limitaciones conocidas y trabajo futuro

* **Limitaciones:** La orquestación de Airflow se encuentra desplegada en modo `standalone`, lo cual no es apto para entornos de alta disponibilidad. Los umbrales de alerta actualmente son estáticos.
* **Trabajo futuro:** Migrar los cálculos estáticos hacia alertas dinámicas basadas en modelos de machine learning, implementar particionado de datos lógico por fecha `date=YYYY-MM-DD` en la capa Curated, e integrar herramientas de visualización de datos de terceros.
## 9. Tabla de contribuciones

| Integrante | Usuario GitHub | Tareas Principales | PR principal |
| --- | --- | --- | --- |
| **Jeison Sanchez** | `Anorack123456789` | Diseño de arquitectura base, Crear Docker Compose, Configuracion Kafka | *[[Enlace a PR #22]](https://github.com/Anorack123456789/RapidGo_Customer_Experience/pull/22)* |
| **Jaime Florez** | `DipUwU` | Pipeline de Spark Streaming, Definicion contrato JSON, Creacion DAG | *[[Enlace a PR #36]](https://github.com/Anorack123456789/RapidGo_Customer_Experience/pull/36)* |
| **Andres Triana** | `feTtros` | Creacion Tablero KanBan, Productor de eventos, Configuracion CI.yml | *[[Enlace a PR #29]](https://github.com/Anorack123456789/RapidGo_Customer_Experience/pull/29)* |
## 10. Ejecuciones manuales Flujo CI.yml
[Ejecuciones manuales](https://github.com/Anorack123456789/RapidGo_Customer_Experience/actions?query=event%3Aworkflow_dispatch)

## 11. Enlace al video final

[Ver RapidGo - Variante 7](https://youtu.be/u70ZptVX73I)
