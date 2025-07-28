import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def log_execution_time(task_name: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"⏱️ Iniciando tarea: {task_name}")
            start = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start
            logger.info(f"✅ {task_name} finalizada en {duration:.2f} segundos")
            return result
        return wrapper
    return decorator
