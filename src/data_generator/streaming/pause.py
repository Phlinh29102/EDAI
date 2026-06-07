"""PauseEventGenerator."""
from datetime import datetime
from typing import Any, Dict

from data_generator.core.config import GeneratorConfig
from data_generator.core.utils import RandomDataUtils
from data_generator.streaming.base_event_generator import BaseEventGenerator


class PauseEventGenerator(BaseEventGenerator):
    """Generate pause streaming events."""

    EVENT_TYPE = "pause"

    def __init__(self, config: GeneratorConfig, utils: RandomDataUtils) -> None:
        super().__init__(config=config, utils=utils)

    def generate_event(self, user_ctx: Dict[str, Any], ts: datetime) -> Dict[str, Any]:
        event = self._base_event(self.EVENT_TYPE, user_ctx, ts)
        event["playback_position_seconds"] = user_ctx.get(
            "playback_position_seconds",
            int(self.utils.rng.integers(30, 3601)),
        )
        return event

    def summary(self) -> Dict[str, Any]:
        summary = super().summary()
        summary["event_type"] = self.EVENT_TYPE
        return summary
