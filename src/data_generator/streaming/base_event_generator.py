"""BaseEventGenerator - abstract base for streaming event generators."""
from abc import abstractmethod
from datetime import date, datetime
from typing import Any, Dict, Optional

from data_generator.core.base_generator import BaseGenerator
from data_generator.core.config import GeneratorConfig
from data_generator.core.utils import RandomDataUtils


class BaseEventGenerator(BaseGenerator):
    """Base class for unified streaming event generators."""

    DEVICE_TYPES = ["smart_tv", "web", "mobile_app"]
    PLATFORMS = ["smart_tv", "web", "mobile_app"]

    def __init__(self, config: GeneratorConfig, utils: RandomDataUtils) -> None:
        """
        Initialize the base event generator.

        Args:
            config (GeneratorConfig): Loaded generator configuration.
            utils (RandomDataUtils): Shared random-data utility instance.
        """
        super().__init__(config=config, utils=utils)
        self.late_arrival_rate = config.get("late_arrival_rate", 0.0)
        delay_range = config.get("late_delay_min_max", [1, 48])
        self.late_delay_min = int(delay_range[0])
        self.late_delay_max = int(delay_range[1])

    @abstractmethod
    def generate_event(self, user_ctx: Dict[str, Any], ts: datetime) -> Dict[str, Any]:
        """
        Generate one event from user context and event timestamp.

        Args:
            user_ctx (Dict[str, Any]): User/session/video context.
            ts (datetime): Event timestamp.

        Returns:
            Dict[str, Any]: Event following the unified streaming schema.
        """
        raise NotImplementedError

    def generate(self) -> Dict[str, Any]:
        """BaseGenerator compatibility wrapper."""
        return self.generate_event({}, datetime.utcnow())

    def summary(self) -> Dict[str, Any]:
        """
        Provide a summary of event generator configuration.

        Returns:
            Dict[str, Any]: Generator configuration.
        """
        return {
            "generator": self.__class__.__name__,
            "late_arrival_rate": self.late_arrival_rate,
            "late_delay_min": self.late_delay_min,
            "late_delay_max": self.late_delay_max,
        }

    def _base_event(
        self,
        event_type: str,
        user_ctx: Dict[str, Any],
        ts: datetime,
    ) -> Dict[str, Any]:
        created_ts = self.utils.generate_late_timestamps(
            ts,
            rate=self.late_arrival_rate,
            delay_min=self.late_delay_min,
            delay_max=self.late_delay_max,
        )
        return {
            "event_id": self._event_id(event_type),
            "event_type": event_type,
            "event_timestamp": self._timestamp_millis(ts),
            "created_ts": self._timestamp_millis(created_ts),
            "user_id": user_ctx.get("user_id", self._default_user_id()),
            "session_id": user_ctx.get("session_id", self._default_session_id()),
            "device_type": user_ctx.get("device_type", self._sample(self.DEVICE_TYPES)),
            "platform": user_ctx.get("platform", self._sample(self.PLATFORMS)),
            "video_id": user_ctx.get("video_id"),
            "genre_id": user_ctx.get("genre_id"),
            "playback_position_seconds": None,
            "ad_campaign_id": None,
            "midpoint": None,
            "third_quartile": None,
        }

    def _event_id(self, event_type: str) -> str:
        value = int(self.utils.rng.integers(1, 10**12))
        return f"{event_type}_{value:012d}"

    def _default_user_id(self) -> str:
        value = int(self.utils.rng.integers(1, 10**8))
        return f"user_{value:08d}"

    def _default_session_id(self) -> str:
        value = int(self.utils.rng.integers(1, 10**10))
        return f"session_{value:010d}"

    def _sample(self, values: list[str]) -> str:
        return str(self.utils.rng.choice(values))

    def _timestamp_millis(self, ts: datetime) -> int:
        return int(ts.timestamp() * 1000)

    def _schema_change_date(self) -> Optional[date]:
        value = self.config.get("schema_change_date")
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if hasattr(value, "isoformat") and not isinstance(value, str):
            return value
        return datetime.fromisoformat(value).date()
