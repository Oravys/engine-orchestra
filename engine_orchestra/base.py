"""Base classes for engine-orchestra tasks and engines."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class EngineResult:
    """Result returned by an engine after execution.

    Attributes:
        engine_name: Identifier of the engine that produced this result.
        confidence: A float in [0.0, 1.0] indicating result confidence.
        scores: Arbitrary numeric scores produced by the engine.
        insights: Arbitrary key-value insights or metadata.
        dependencies_met: Whether all declared dependencies were satisfied.
    """

    engine_name: str
    confidence: float = 0.0
    scores: Dict[str, float] = field(default_factory=dict)
    insights: Dict[str, Any] = field(default_factory=dict)
    dependencies_met: bool = True

    def __repr__(self) -> str:
        met = "OK" if self.dependencies_met else "DEGRADED"
        return (
            f"EngineResult(engine={self.engine_name!r}, "
            f"confidence={self.confidence:.4f}, deps={met})"
        )


class BaseEngine(ABC):
    """Abstract base class for class-based engines.

    Subclasses must define ``NAME`` and implement ``analyze()``.
    Optionally set ``DEPENDENCIES`` to declare other engine names that
    must complete before this engine runs.
    """

    NAME: str = ""
    DEPENDENCIES: List[str] = []

    def get_name(self) -> str:
        """Return the engine name."""
        return self.NAME or self.__class__.__name__

    @abstractmethod
    def analyze(self, context: Dict[str, Any]) -> Optional[EngineResult]:
        """Execute the engine logic.

        Args:
            context: Shared context dictionary. Engines may read from or
                write to this dict to share data across the pipeline.

        Returns:
            An EngineResult, or None if the engine has nothing to report.
        """
        ...

    def safe_analyze(self, context: Dict[str, Any]) -> Optional[EngineResult]:
        """Run analyze() with exception handling.

        Returns None and logs the error if analyze() raises.
        """
        import logging

        try:
            return self.analyze(context)
        except Exception as exc:
            logging.getLogger(__name__).error(
                "[ENGINE] %s crashed: %s", self.get_name(), exc
            )
            return None
