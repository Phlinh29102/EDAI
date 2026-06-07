"""Test BaseGenerator."""
from typing import Any, Dict

import pytest

from data_generator.core.base_generator import BaseGenerator
from data_generator.core.utils import RandomDataUtils


class ConcreteGenerator(BaseGenerator):
    def generate(self) -> list[int]:
        return [1]

    def summary(self) -> Dict[str, Any]:
        return {"generator": self.__class__.__name__}


def test_base_generator_cannot_be_instantiated(dummy_config):
    with pytest.raises(TypeError):
        BaseGenerator(config=dummy_config, utils=RandomDataUtils(seed=42))


def test_base_generator_stores_shared_state(dummy_config):
    utils = RandomDataUtils(seed=42)
    generator = ConcreteGenerator(config=dummy_config, utils=utils)

    assert generator.config is dummy_config
    assert generator.utils is utils
    assert generator.generate() == [1]
    assert generator.summary() == {"generator": "ConcreteGenerator"}
