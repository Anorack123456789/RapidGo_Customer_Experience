import pytest
from producer.reglas import evaluar_calificacion, evaluar_concentracion_reclamos

# --- Pruebas para Regla 1 ---
def test_evaluar_calificacion_critica():
    assert evaluar_calificacion(1) == "ALERTA_CRITICA"
    assert evaluar_calificacion(2) == "ALERTA_CRITICA"

def test_evaluar_calificacion_atencion_vs_critica():
    # 3 estrellas con motivo grave es CRITICA
    assert evaluar_calificacion(3, "demora") == "ALERTA_CRITICA"
    assert evaluar_calificacion(3, "daño") == "ALERTA_CRITICA"
    # 3 estrellas con motivo leve es ATENCION
    assert evaluar_calificacion(3, "clima") == "ATENCION"

def test_evaluar_calificacion_normal():
    assert evaluar_calificacion(4) == "NORMAL"
    assert evaluar_calificacion(5) == "NORMAL"

def test_evaluar_calificacion_fuera_de_rango():
    with pytest.raises(ValueError):
        evaluar_calificacion(6)

# --- Pruebas para Regla 2 ---
def test_evaluar_concentracion_roja():
    # 20 quejas en 2 horas = tasa de 10
    assert evaluar_concentracion_reclamos(20, 2) == "ALERTA_ROJA"

def test_evaluar_concentracion_amarilla():
    # 12 quejas en 2 horas = tasa de 6
    assert evaluar_concentracion_reclamos(12, 2) == "ALERTA_AMARILLA"

def test_evaluar_concentracion_normal():
    # 5 quejas en 2 horas = tasa de 2.5
    assert evaluar_concentracion_reclamos(5, 2) == "NORMAL"