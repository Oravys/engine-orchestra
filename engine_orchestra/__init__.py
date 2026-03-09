"""engine-orchestra -- dependency-aware parallel task execution framework.

Organize tasks into engines with explicit dependencies, then execute them
in parallel phases determined by topological sort.

Quick start::

    from engine_orchestra import Orchestra, BaseEngine, EngineResult

    orch = Orchestra(max_workers=4)
    orch.register("fetch", fetch_func)
    orch.register("process", process_func, dependencies=["fetch"])
    results = orch.execute({"url": "https://example.com"})
"""

from .base import BaseEngine, EngineResult
from .executor import Orchestra, ParallelExecutor
from .plan import ExecutionPlan

__all__ = [
    "BaseEngine",
    "EngineResult",
    "ExecutionPlan",
    "Orchestra",
    "ParallelExecutor",
]

__version__ = "0.1.0"
