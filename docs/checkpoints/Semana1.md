# Checkpoint Semana 1

## 1. Avance de la semana

Se definió la arquitectura inicial del pipeline RapidGo Customer Experience Analytics, correspondiente a la variante 7, centrada en el análisis de la experiencia del cliente. También se establecieron los campos del evento JSON, los tres indicadores de satisfacción, las dos reglas iniciales de alerta y la organización del repositorio y del tablero Kanban.

## 2. Evidencias

Se documentaron el contrato de eventos, la arquitectura propuesta y la decisión inicial sobre la clave y las particiones de Kafka. Las evidencias definitivas corresponderán a los archivos del repositorio, los Issues creados y el tablero de trabajo.

## 3. Decisión técnica

Se propuso utilizar `order_id` como clave del topic `rapidgo.customer_feedback`, con tres particiones, para agrupar los eventos de un mismo pedido y permitir el procesamiento paralelo. La decisión se registrará en ADR-001 y podrá revisarse después de las primeras ejecuciones.

## 4. Riesgo principal

El principal riesgo es que la generación de eventos no represente adecuadamente la variedad de calificaciones y motivos de reclamo, lo que podría limitar las pruebas de los indicadores y las alertas. Para reducirlo, se definirá un conjunto de eventos de prueba con calificaciones, motivos y marcas temporales variadas.

## 5. Plan para la semana 2

Implementar el generador de eventos en Python, configurar Kafka y el topic correspondiente, desarrollar el procesamiento inicial con Spark Structured Streaming, calcular los indicadores, implementar las reglas de alerta y comenzar el almacenamiento en Parquet. También se incorporarán pruebas automatizadas y GitHub Actions para validar el código y el contrato de los eventos.
