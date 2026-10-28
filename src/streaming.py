from pyspark.sql import SparkSession, functions as F, types as T
import time

TOPIC = "rapidgo.customer_feedback"
KAFKA = "kafka:19092"

DATALAKE_DIR = "/opt/spark/work-dir/datalake"
CHECKPOINTS_DIR = "/opt/spark/work-dir/docs/checkpoints"

spark = (SparkSession.builder
    .appName("RapidGo_CustomerExperience")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("rating", T.IntegerType()),
    T.StructField("motivo", T.StringType()),
    T.StructField("timestamp_ms", T.LongType()),
])

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load())

parsed = (raw
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS json_value",
        "topic", "partition", "offset")
    .withColumn("data", F.from_json("json_value", schema))
    .select("kafka_key", "topic", "partition", "offset", "data.*")
    .withColumn("event_time", F.to_timestamp(F.col("timestamp_ms") / 1000)))

processed = (parsed
    .withColumn(
        "nivel_alerta",
        F.when(F.col("rating") <= 2, "ALERTA_CRITICA")
         .when((F.col("rating") == 3) & F.col("motivo").isin("demora", "daño", "perdida"), "ALERTA_CRITICA")
         .when(F.col("rating") == 3, "ATENCION")
         .otherwise("NORMAL"))
    .withColumn(
        "causa_alerta",
        F.when(F.col("rating") <= 2, "CALIFICACION_BAJA")
         .when(F.col("nivel_alerta") == "ALERTA_CRITICA", "MOTIVO_GRAVE")
         .otherwise("SIN_ALERTA")))

EVENTOS = f"{DATALAKE_DIR}/eventos_procesados"
ALERTAS = f"{DATALAKE_DIR}/alertas"
CP_EVENTOS = f"{CHECKPOINTS_DIR}/eventos"
CP_METRICAS = f"{CHECKPOINTS_DIR}/metricas"

def procesar_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.persist()
    print(f"\n=== MICRO-BATCH {batch_id} ===")
    batch_df.groupBy("partition").agg(
        F.min("offset").alias("offset_min"),
        F.max("offset").alias("offset_max"),
        F.count("*").alias("eventos")
    ).orderBy("partition").show(truncate=False)
    
    batch_df.write.mode("append").parquet(EVENTOS)

    alertas = batch_df.filter(F.col("nivel_alerta") != "NORMAL")
    if not alertas.isEmpty():
        alertas.write.mode("append").parquet(ALERTAS)
        print("Alertas en el micro-batch:", alertas.count())
    batch_df.unpersist()

q_eventos = (processed.writeStream
    .foreachBatch(procesar_batch)
    .option("checkpointLocation", CP_EVENTOS)
    .trigger(processingTime="5 seconds")
    .start())

metricas = (processed
    .withWatermark("event_time", "30 seconds")
    .groupBy(F.window("event_time", "20 seconds"), "motivo")
    .agg(
        F.count("*").alias("total_quejas"),
        F.sum(F.when(F.col("nivel_alerta") == "ALERTA_CRITICA", 1).otherwise(0)).alias("criticas")
    ))

q_metricas = (metricas.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .option("numRows", 50)
    .option("checkpointLocation", CP_METRICAS)
    .trigger(processingTime="10 seconds")
    .start())

try:
    while True:
        time.sleep(10)
        progreso = q_eventos.lastProgress
        if progreso:
            print("PROGRESO:", {
                "batchId": progreso.get("batchId"),
                "processedRowsPerSecond": progreso.get("processedRowsPerSecond"),
            })
except KeyboardInterrupt:
    print("Deteniendo consultas...")
finally:
    q_eventos.stop()
    q_metricas.stop()
    spark.stop()