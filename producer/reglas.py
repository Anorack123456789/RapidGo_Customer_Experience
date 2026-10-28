def evaluar_calificacion(rating: int, motivo: str = "") -> str:
    """
    Regla 1: Evalúa la calificación individual de un cliente y determina el nivel de alerta.
    """
    if rating < 1 or rating > 5:
        raise ValueError("La calificación debe estar entre 1 y 5 estrellas")
    
    if rating <= 2:
        return "ALERTA_CRITICA"
    elif rating == 3:
        motivos_graves = ["demora", "daño", "perdida"]
        if motivo.lower() in motivos_graves:
            return "ALERTA_CRITICA"
        return "ATENCION"
    else:
        return "NORMAL"


def evaluar_concentracion_reclamos(cantidad_reclamos: int, horas: int) -> str:
    """
    Regla 2: Evalúa si hay un pico inusual de quejas en un periodo de tiempo.
    """
    if cantidad_reclamos < 0:
        raise ValueError("La cantidad de reclamos no puede ser negativa")
    if horas <= 0:
        raise ValueError("El periodo en horas debe ser mayor a cero")
        
    tasa_quejas = cantidad_reclamos / horas
    
    if tasa_quejas >= 10:
        return "ALERTA_ROJA"
    elif tasa_quejas >= 5:
        return "ALERTA_AMARILLA"
    else:
        return "NORMAL"