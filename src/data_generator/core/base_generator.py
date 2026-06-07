"""BaseGenerator - abstract base class for all generators."""
from abc import ABC, abstractmethod
from typing import Any, Dict

from data_generator.core.config import GeneratorConfig
from data_generator.core.utils import RandomDataUtils


class BaseGenerator(ABC):
    """Common interface for offline and streaming generators."""

    def __init__(self, config: GeneratorConfig, utils: RandomDataUtils) -> None:
        """
        Initialize shared generator state.

        Args:
            config (GeneratorConfig): Loaded generator configuration.
            utils (RandomDataUtils): Shared random-data utility instance.
        """
        self.config = config
        self.utils = utils

    @abstractmethod
    def generate(self) -> Any:
        """
        Generate records for the concrete generator.

        Returns:
            Any: Generated records. Concrete subclasses define the exact type.
        """
        raise NotImplementedError

    @abstractmethod
    def summary(self) -> Dict[str, Any]:
        """
        Provide a summary of generator state.

        Returns:
            Dict[str, Any]: Generator state and configuration.
        """
        raise NotImplementedError
