class ValidationRejectFileError(Exception):
    def __init__(self, message: str, issues: list[dict] | None = None):
        super().__init__(message)
        self.issues = issues or []

class RecoverableError(Exception):
    """
    Excepción para errores que se consideran temporales y para los que
    un reintento de la tarea tiene sentido (ej. fallos de red, APIs no disponibles).
    """
    def __init__(self, message, status=None, error_text=None, target=None):
        super().__init__(message)
        self.status = status
        self.error_text = error_text
        self.target = target
    pass

class HttpSenderError(Exception):
    """
    Excepción para errores ocurridos durante el envío a un destino HTTP.
    Transporta el código de estado y el cuerpo de la respuesta.
    """
    def __init__(self, message, status_code=None, response_text=None, target=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text
        self.target = target

    def __str__(self):
        return f"{super().__str__()} [Status: {self.status_code}]"
