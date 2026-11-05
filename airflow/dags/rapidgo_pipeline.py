import glob
import os
import uuid

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
import pyarrow as pa
import pyarrow.parquet as pq

from airflow.sdk import dag, task


# ---------------------------------------------------------
# CONFIGURACION
# ---------------------------------------------------------

RAW_PATH = Path(
    os.getenv(
        "RAPIDGO_RAW_PATH",
        "/opt/airflow/datalake/eventos_procesados"
    )
)

CURATED_PATH = Path(
    os.getenv(
        "RAPIDGO_CURATED_PATH",
        "/opt/airflow/datalake/curated"
    )
)

REPORT_PATH = Path(
    os.getenv(
        "RAPIDGO_REPORT_PATH",
        "/opt/airflow/datalake/reports"
    )
)

REQUIRED_COLUMNS = [
    "order_id",
    "rating",
    "reason",
    "timestamp",
    "event_time",
    #"columna_falsa_para_fallo",
]

AGGREGATION_COLUMNS = [
    "rating",
    "reason",
    "event_time",
]


# ---------------------------------------------------------
# UTILIDADES
# ---------------------------------------------------------

def get_parquet_files(directory: Path) -> list[str]:
    """Obtiene los archivos Parquet generados por Spark."""

    return sorted(
        file for file in glob.glob(
            str(directory / "**" / "*.parquet"),
            recursive=True
        )
        if os.path.isfile(file)
        and os.path.basename(file).startswith("part-")
    )


def write_parquet(rows, file_path, schema):
    """Escribe registros en formato Parquet."""

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    table = pa.Table.from_pylist(rows, schema=schema)

    pq.write_table(table, file_path, compression="snappy")


def read_event_rows(files):
    """Lee los campos necesarios para calcular los indicadores."""

    for file in files:
        parquet_file = pq.ParquetFile(file)

        for batch in parquet_file.iter_batches(
            columns=AGGREGATION_COLUMNS,
            batch_size=10000
        ):
            yield from batch.to_pylist()


def _normalize_event_time(value):
    """# FIX: convierte a datetime con tz UTC para comparaciones seguras."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime):
        raise TypeError(
            f"event_time no es datetime: {value!r} ({type(value)})"
        )
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

@dag(
    dag_id="rapidgo_customer_experience_pipeline",
    description="Validación, agregación y generación de reportes de RapidGo",
    start_date=pendulum.datetime(
        2026, 10, 1, tz="America/Bogota"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "rapidgo-team",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["rapidgo", "customer-experience", "semana-3"],
)
def rapidgo_pipeline():

    # -----------------------------------------------------
    # TASK 1: CHECK SOURCES
    # -----------------------------------------------------

    @task
    def check_sources():

        if not RAW_PATH.exists():
            raise FileNotFoundError(
                f"No existe el directorio de entrada: {RAW_PATH}"
            )

        files = get_parquet_files(RAW_PATH)

        if not files:
            raise FileNotFoundError(
                f"No se encontraron archivos Parquet en {RAW_PATH}"
            )

        result = {
            "input_path": str(RAW_PATH),
            "file_count": len(files),
        }

        print("Fuentes disponibles:", result)

        return result

    # -----------------------------------------------------
    # TASK 2: VALIDATE RAW
    # -----------------------------------------------------

    @task
    def validate_raw(source_info):

        files = get_parquet_files(
            Path(source_info["input_path"])
        )

        if not files:
            raise FileNotFoundError(
                "No hay archivos Parquet para validar"
            )

        total_records = 0
        invalid_records = 0

        for file in files:

            parquet_file = pq.ParquetFile(file)

            columns = set(parquet_file.schema.names)
            missing = set(REQUIRED_COLUMNS) - columns

            if missing:
                raise ValueError(
                    f"Columnas faltantes en {file}: {missing}"
                )

            for batch in parquet_file.iter_batches(
                columns=REQUIRED_COLUMNS,
                batch_size=10000
            ):
                for row in batch.to_pylist():

                    total_records += 1

                    order_id = row["order_id"]
                    rating = row["rating"]
                    reason = row["reason"]
                    timestamp = row["timestamp"]
                    event_time = row["event_time"]

                    valid = (
                        order_id is not None
                        and str(order_id).strip() != ""
                        and isinstance(rating, (int, float))
                        and not isinstance(rating, bool)
                        and 1 <= float(rating) <= 5
                        and reason is not None
                        and str(reason).strip() != ""
                        and timestamp is not None
                        and str(timestamp).strip() != ""
                        and isinstance(event_time, datetime)
                    )

                    if not valid:
                        invalid_records += 1

        if total_records == 0:
            raise ValueError(
                "Los archivos Parquet no contienen registros"
            )

        if invalid_records > 0:
            raise ValueError(
                f"Se encontraron {invalid_records} registros inválidos "
                f"de un total de {total_records}"
            )

        result = {
            "input_path": source_info["input_path"],
            "file_count": len(files),
            "total_records": total_records,
            "valid_records": total_records - invalid_records,
            "invalid_records": invalid_records,
        }

        print("Validación RAW:", result)

        return result

    # -----------------------------------------------------
    # TASK 3: AGGREGATE KPIS
    # -----------------------------------------------------

    @task
    def aggregate_kpis(validation_info):

        files = get_parquet_files(
            Path(validation_info["input_path"])
        )

        rating_totals = defaultdict(
            lambda: {"sum": 0.0, "count": 0}
        )

        negative_reasons = defaultdict(int)

        for row in read_event_rows(files):

            rating = float(row["rating"]) 
            reason = str(row["reason"]).strip().lower()

            event_time = _normalize_event_time(row["event_time"])

            minute = event_time.replace(second=0, microsecond=0)

            rating_totals[minute]["sum"] += rating
            rating_totals[minute]["count"] += 1

            if rating <= 2:
                negative_reasons[(minute, reason)] += 1

        # KPI 1: calificación media por minuto.
        rating_rows = []

        for minute, values in sorted(rating_totals.items()):

            average = values["sum"] / values["count"]

            rating_rows.append({
                "window_start": minute.isoformat(),
                "average_rating": average,
                "total_opinions": values["count"],
            })

        # KPI 2: opiniones negativas por motivo.
        reason_rows = [
            {
                "window_start": minute.isoformat(),
                "motivo": reason,
                "negative_count": count,
            }
            for (minute, reason), count
            in sorted(negative_reasons.items())
        ]

        # KPI 3: tendencia entre ventanas consecutivas.
        trend_rows = []

        ordered_minutes = sorted(rating_totals.keys())

        for index in range(1, len(ordered_minutes)):

            previous_minute = ordered_minutes[index - 1]
            current_minute = ordered_minutes[index]

            if current_minute - previous_minute != timedelta(minutes=1):
                continue

            previous_average = (
                rating_totals[previous_minute]["sum"]
                / rating_totals[previous_minute]["count"]
            )

            current_average = (
                rating_totals[current_minute]["sum"]
                / rating_totals[current_minute]["count"]
            )

            trend_rows.append({
                "window_start": current_minute.isoformat(),
                "average_rating": current_average,
                "previous_average": previous_average,
                "delta_points": current_average - previous_average,
            })

        # -------------------------------------------------
        # ALERTAS
        # -------------------------------------------------

        alerts = []

        # A01: caída de satisfacción.
        for row in rating_rows:

            if row["average_rating"] < 3.5:
                alerts.append({
                    "alert_type": "caida_satisfaccion",
                    "window_start": row["window_start"],
                    "observed_value": row["average_rating"],
                    "threshold": 3.5,
                    "motivo": None,
                })

        # A02: concentración de reclamos.
        for row in reason_rows:

            if row["negative_count"] >= 5:
                alerts.append({
                    "alert_type": "concentracion_reclamos",
                    "window_start": row["window_start"],
                    "observed_value": float(row["negative_count"]),
                    "threshold": 5.0,
                    "motivo": row["motivo"],
                })

        # Identificador para separar las ejecuciones.
        run_id = (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            + "_"
            + uuid.uuid4().hex[:8]
        )

        output_dir = CURATED_PATH / f"airflow_run={run_id}"
        os.makedirs(output_dir, exist_ok=True)

        # Esquemas explícitos para permitir resultados vacíos.
        rating_schema = pa.schema([
            ("window_start", pa.string()),
            ("average_rating", pa.float64()),
            ("total_opinions", pa.int64()),
        ])

        reason_schema = pa.schema([
            ("window_start", pa.string()),
            ("motivo", pa.string()),
            ("negative_count", pa.int64()),
        ])

        trend_schema = pa.schema([
            ("window_start", pa.string()),
            ("average_rating", pa.float64()),
            ("previous_average", pa.float64()),
            ("delta_points", pa.float64()),
        ])

        alert_schema = pa.schema([
            ("alert_type", pa.string()),
            ("window_start", pa.string()),
            ("observed_value", pa.float64()),
            ("threshold", pa.float64()),
            ("motivo", pa.string()),
        ])

        # Almacenamiento de la capa Curated.
        write_parquet(
            rating_rows,
            str(output_dir / "kpi_rating.parquet"),
            rating_schema
        )

        write_parquet(
            reason_rows,
            str(output_dir / "kpi_negative_reasons.parquet"),
            reason_schema
        )

        write_parquet(
            trend_rows,
            str(output_dir / "kpi_satisfaction_trend.parquet"),
            trend_schema
        )

        write_parquet(
            alerts,
            str(output_dir / "alerts.parquet"),
            alert_schema
        )

        result = {
            "run_id": run_id,
            "output_dir": str(output_dir),
            "source_records": validation_info["total_records"],
            "rating_windows": len(rating_rows),
            "reason_groups": len(reason_rows),
            "trend_windows": len(trend_rows),
            "alerts_generated": len(alerts),
        }

        print("Resultados de agregación:", result)

        return result

    # -----------------------------------------------------
    # TASK 4: VALIDATE CURATED
    # -----------------------------------------------------

    @task
    def validate_curated(aggregation_info):

        output_dir = Path(aggregation_info["output_dir"])

        required_files = [
            "kpi_rating.parquet",
            "kpi_negative_reasons.parquet",
            "kpi_satisfaction_trend.parquet",
            "alerts.parquet",
        ]

        results = {}

        for filename in required_files:

            file_path = output_dir / filename

            if not file_path.exists():
                raise FileNotFoundError(
                    f"No se encontró el resultado: {file_path}"
                )

            parquet_file = pq.ParquetFile(file_path)

            results[filename] = {
                "rows": parquet_file.metadata.num_rows,
                "columns": parquet_file.schema.names,
            }

        # Debe existir al menos un indicador de calificación.
        if results["kpi_rating.parquet"]["rows"] == 0:
            raise ValueError(
                "La salida de calificaciones está vacía"
            )

        result = {
            "output_dir": str(output_dir),
            "validated_files": results,
            "quality_status": "PASSED",
        }

        print("Validación CURATED:", result)

        return result

    # -----------------------------------------------------
    # TASK 5: GENERATE REPORT
    # -----------------------------------------------------

    @task
    def generate_report(source_info, validation_info,
                        aggregation_info, curated_info):

        os.makedirs(REPORT_PATH, exist_ok=True)

        report_file = (
            REPORT_PATH
            / f"rapidgo_report_{aggregation_info['run_id']}.md"
        )

        report = f"""# Reporte de experiencia del cliente - RapidGo

## Información de ejecución

- Identificador: {aggregation_info['run_id']}
- Fecha: {datetime.now(timezone.utc).isoformat()}
- Estado de calidad: {curated_info['quality_status']}

## Fuentes

- Directorio: {source_info['input_path']}
- Archivos Parquet: {source_info['file_count']}
- Registros examinados: {validation_info['total_records']}
- Registros válidos: {validation_info['valid_records']}
- Registros inválidos: {validation_info['invalid_records']}

## Indicadores

- Ventanas de calificación media: {aggregation_info['rating_windows']}
- Grupos de motivos negativos: {aggregation_info['reason_groups']}
- Ventanas de tendencia calculadas: {aggregation_info['trend_windows']}

## Alertas

- Alertas generadas: {aggregation_info['alerts_generated']}

## Resultados

- Directorio Curated: {aggregation_info['output_dir']}
- Archivos validados: {len(curated_info['validated_files'])}
"""

        report_file.write_text(report, encoding="utf-8")

        print(f"Reporte generado: {report_file}")

        return {
            "report_path": str(report_file),
            "status": "SUCCESS",
        }

    # -----------------------------------------------------
    # DEPENDENCIAS DEL DAG
    # -----------------------------------------------------

    sources = check_sources()

    raw_validation = validate_raw(sources)

    aggregation = aggregate_kpis(raw_validation)

    curated_validation = validate_curated(aggregation)

    generate_report(
        sources,
        raw_validation,
        aggregation,
        curated_validation
    )


rapidgo_pipeline()