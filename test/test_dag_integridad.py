import pytest
from airflow.models import DagBag

def test_dag_carga_sin_errores():
    """Valida que el DAG se importa correctamente sin errores de compilación."""
    dag_bag = DagBag(dag_folder='airflow/dags/', include_examples=False)
    
    assert len(dag_bag.import_errors) == 0, f"Errores al importar DAGs: {dag_bag.import_errors}"
    assert 'procesamiento_experiencia_cliente' in dag_bag.dags
    
    dag = dag_bag.dags['procesamiento_experiencia_cliente']
    assert len(dag.tasks) == 5, "El DAG debe tener exactamente 5 tareas."