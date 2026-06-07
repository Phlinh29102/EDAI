"""DataHub lineage emission interface."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List


@dataclass(frozen=True)
class LineageEvent:
    """Dataset-level lineage relation."""

    job_name: str
    inputs: List[str]
    outputs: List[str]


class DataHubEmitter:
    """Minimal interface for emitting DataHub lineage events."""

    def __init__(self, enabled: bool = False) -> None:
        self.enabled = enabled
        self._events: List[LineageEvent] = []

    def emit_lineage(
        self,
        job_name: str,
        inputs: Iterable[str],
        outputs: Iterable[str],
    ) -> LineageEvent:
        """Record or emit a lineage event."""
        event = LineageEvent(
            job_name=job_name,
            inputs=list(inputs),
            outputs=list(outputs),
        )
        self._events.append(event)
        if self.enabled:
            raise NotImplementedError("DataHub network emission is not configured yet")
        return event

    def summary(self) -> Dict[str, object]:
        """Return emitted lineage event metadata."""
        return {
            "enabled": self.enabled,
            "event_count": len(self._events),
            "events": [event.__dict__ for event in self._events],
        }
