# RapidGo Customer Experience

## Variante 7

## Descripción del proyecto

RapidGo Customer Experience Analytics es un proyecto orientado al análisis de la experiencia del cliente en una plataforma de entregas. La solución busca identificar cambios en la satisfacción, reconocer los motivos más frecuentes de insatisfacción y generar alertas ante situaciones que puedan afectar la percepción del servicio.

## Problema de negocio

El deterioro de la satisfacción de los clientes puede manifestarse mediante calificaciones bajas y concentraciones de reclamos relacionados con demoras, pedidos incompletos, productos dañados o problemas de atención. El análisis de estos eventos permitirá reconocer patrones y proporcionar información útil para la toma de decisiones.

## Objetivo

Diseñar e implementar un pipeline de Big Data capaz de capturar, procesar, almacenar y analizar eventos de experiencia del cliente, calculando indicadores de satisfacción y generando alertas mediante reglas definidas por el equipo.

## Resumen

- **Fuente de eventos:** Generador de opiniones y reclamos.
- **Transporte:** Apache Kafka.
- **Procesamiento:** Apache Spark Structured Streaming.
- **Almacenamiento:** Data Lake con capas Raw, Processed y Curated.
- **Orquestación:** Apache Airflow.
- **Gestión del desarrollo:** Git, GitHub, Kanban y GitHub Actions.

## Indicadores KPIs

| KPI | Unidad de medida | Ventana inicial | Propósito |
|---|---|---|---|
| Calificación media del cliente | Puntos de 1 a 5 | 1 minuto | Evaluar la satisfacción general |
| Motivos frecuentes de insatisfacción | Cantidad de opiniones negativas por motivo | 1 minuto | Identificar problemas recurrentes |
| Tendencia de satisfacción | Diferencia en puntos | Dos ventanas consecutivas de 1 minuto | Detectar cambios en la satisfacción |

## Alertas

|Nombre de la alerta | Objetivo | Condición de activación | Datos utilizados | Acción esperada | Ejemplo |
|---|---|---|---|---|---|
| Caída de satisfacción | Detectar una disminución en la satisfacción general de los clientes. | La calificación media de una ventana de un minuto es inferior a 3,5 sobre 5. | Calificaciones (`rating`) y marcas temporales (`timestamp`). | Generar una alerta que registre la calificación media obtenida y la ventana temporal en la que se detectó la condición. | Si la calificación media durante un minuto es de 3,2, se activa la alerta de caída de satisfacción. |
| Concentración de reclamos | Identificar un incremento de opiniones negativas asociadas con un mismo motivo. | Se registran cinco o más opiniones con calificación igual o inferior a 2, asociadas con el mismo motivo, durante una ventana de un minuto. | Calificaciones (`rating`), motivos (`reason`) y marcas temporales (`timestamp`). | Generar una alerta que identifique el motivo del reclamo y la cantidad de opiniones negativas registradas. | Si durante un minuto se reciben seis opiniones negativas por demoras, se activa la alerta de concentración de reclamos. |

## Arquitectura

Los eventos serán generados en formato JSON y publicados en Kafka; posteriormente, Spark los validará y procesará para calcular los indicadores y detectar las condiciones de alerta; los datos se almacenarán en el Data Lake y Airflow permitirá orquestar las tareas de validación, agregación y generación de reportes.

## Estado del proyecto

**Primera semana:** Diseño de arquitectura, definición del contrato de eventos, especificación de KPIs y alertas, organización del repositorio y documentación de decisiones técnicas.

## Integrantes

- Integrante 1: Jeison Sánchez Buitrago.
- Integrante 2: Jaime Alberto Florez Paez.
- Integrante 3: Andrés Felipe Triana Ospina.
