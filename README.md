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
