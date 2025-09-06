import logging

def setup_logger():
    """
    Configura un logger estándar para el proyecto.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger("ProyectoELT")

