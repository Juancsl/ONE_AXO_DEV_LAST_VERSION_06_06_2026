# En: oneaxo_cfg_pkg/sources/base_source.py
from abc import ABC, abstractmethod

class BaseSourceHandler(ABC):
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def discover(self) -> list[dict]:
        """
        Descubre nuevos trabajos (archivos) y devuelve una lista de jobs.
        """
        pass