# Nuevo dynamic_loader.py
from importlib import import_module

def load_handler_instance(module_path: str, handler_class: str, **kwargs):
    """
    Carga dinámicamente una clase y la instancia, pasando cualquier
    argumento de palabra clave adicional a su constructor.
    """
    module = import_module(module_path)
    cls = getattr(module, handler_class)
    # Pasa los argumentos de palabra clave al constructor de la clase
    return cls(**kwargs) 