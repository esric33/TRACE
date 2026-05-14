from .support import ExecError, load_extract_store


def execute_dag(*args, **kwargs):
    from .runtime import execute_dag as _execute_dag

    return _execute_dag(*args, **kwargs)

__all__ = [
    "ExecError",
    "execute_dag",
    "load_extract_store",
]
