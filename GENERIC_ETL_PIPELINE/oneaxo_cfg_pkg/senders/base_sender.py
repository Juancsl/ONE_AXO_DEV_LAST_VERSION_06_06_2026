from abc import ABC, abstractmethod

class BaseSender(ABC):
    @abstractmethod
    def send(self, payload: bytes, config: dict, **kwargs):
        """
        Envía el payload al destino.
        Debe ser implementado por cada clase de sender.
        """
        pass