"""AdImpressionEventGenerator."""
from datetime import datetime
from typing import Any, Dict

from data_generator.core.config import GeneratorConfig
from data_generator.core.utils import RandomDataUtils
from data_generator.streaming.base_event_generator import BaseEventGenerator


class AdImpressionEventGenerator(BaseEventGenerator):
    """Generate ad_impression streaming events."""

    EVENT_TYPE = "ad_impression"

    def __init__(self, config: GeneratorConfig, utils: RandomDataUtils) -> None:
        super().__init__(config=config, utils=utils)

    def generate_event(self, user_ctx: Dict[str, Any], ts: datetime) -> Dict[str, Any]:
        event = self._base_event(self.EVENT_TYPE, user_ctx, ts)
        event["ad_campaign_id"] = user_ctx.get(
            "ad_campaign_id",
            self._default_ad_campaign_id(),
        )

        schema_change_date = self._schema_change_date()
        if schema_change_date is None or ts.date() >= schema_change_date:
            reached_midpoint = bool(self.utils.rng.random() < 0.72)
            event["midpoint"] = reached_midpoint
            event["third_quartile"] = bool(
                reached_midpoint and self.utils.rng.random() < 0.68
            )

        return event

    def summary(self) -> Dict[str, Any]:
        summary = super().summary()
        summary["event_type"] = self.EVENT_TYPE
        summary["schema_change_date"] = self.config.get("schema_change_date")
        return summary

    def _default_ad_campaign_id(self) -> str:
        value = int(self.utils.rng.integers(1, 10**6))
        return f"campaign_{value:06d}"
