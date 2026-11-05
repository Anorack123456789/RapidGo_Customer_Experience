from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'rapidgo_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2, # Requisito: Reintentos configurados
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    'procesamiento_experiencia_cliente',
    default_args=default_args,
    description='DAG para procesar calificaciones de RapidGo',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Task 1: Validar fuente de datos
    verificar_datalake = BashOperator(
        task_id='verificar_datalake',
        bash_command='ls /opt/airflow/datalake/ || echo "Directorio simulado OK"'
    )

    # Task 2: Tarea de Calidad de Datos con fallo controlado
    def validar_calidad():
        # Genera un fallo el 50% de las veces para demostrar resiliencia y retries
        if random.choice([True, False]):
            raise ValueError("Fallo de calidad de datos simulado: Archivo corrupto.")
        print("Calidad de datos aprobada.")
        return "Calidad OK"

    calidad_datos = PythonOperator(
        task_id='calidad_datos',
        python_callable=validar_calidad,
    )

    # Task 3: Generar Reporte y usar XCom
    def calcular_kpis(ti):
        reseñas_criticas = random.randint(10, 50)
        print(f"Reporte generado: {reseñas_criticas} reseñas críticas.")
        ti.xcom_push(key='total_criticas', value=reseñas_criticas)

    generar_reporte = PythonOperator(
        task_id='generar_reporte',
        python_callable=calcular_kpis,
    )

    # Task 4: Evaluar alertas recibiendo datos por XCom
    def evaluar_xcom_kpis(ti):
        alertas = ti.xcom_pull(task_ids='generar_reporte', key='total_criticas')
        if alertas > 30:
            print(f"ALERTA ROJA: {alertas} quejas graves. Umbral superado.")
        else:
            print(f"Operación normal. {alertas} quejas graves.")

    evaluar_alertas = PythonOperator(
        task_id='evaluar_alertas',
        python_callable=evaluar_xcom_kpis,
    )

    # Task 5: Reporting final
    notificar_fin = BashOperator(
        task_id='notificar_fin',
        bash_command='echo "Pipeline de Experiencia del Cliente finalizado."'
    )

    # Definición de dependencias
    verificar_datalake >> calidad_datos >> generar_reporte >> evaluar_alertas >> notificar_fin