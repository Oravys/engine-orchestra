"""Execution planning with topological sort into parallel phases."""

import logging
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class ExecutionPlan:
    """Builds a dependency-aware execution plan that groups tasks into phases.

    Tasks within a phase have no inter-dependencies and can run in parallel.
    Phases execute sequentially: phase N tasks only depend on tasks from
    phases 0 through N-1.

    Example::

        plan = ExecutionPlan()
        plan.register("parse", parse_func)
        plan.register("validate", validate_func, dependencies=["parse"])
        plan.register("transform", transform_func, dependencies=["parse"])
        plan.register("aggregate", aggregate_func, dependencies=["validate", "transform"])

        phases = plan.build_phases()
        # phases == [["parse"], ["transform", "validate"], ["aggregate"]]
    """

    def __init__(self) -> None:
        self._registry: Dict[str, Callable] = {}
        self._deps: Dict[str, List[str]] = {}

    @property
    def task_names(self) -> List[str]:
        """Return all registered task names."""
        return list(self._registry.keys())

    def register(
        self,
        name: str,
        func: Callable,
        dependencies: Optional[List[str]] = None,
    ) -> None:
        """Register a task with optional dependencies.

        Args:
            name: Unique task identifier.
            func: Callable to execute for this task.
            dependencies: List of task names that must complete before this one.
        """
        self._registry[name] = func
        self._deps[name] = dependencies or []

    def build_phases(self) -> List[List[str]]:
        """Topological-sort tasks into execution phases.

        Tasks whose dependencies are all resolved are placed into the
        current phase.  If a circular dependency is detected (no tasks
        can be resolved), the remaining tasks are forced into a final
        phase and a warning is logged.

        Returns:
            A list of phases, where each phase is a sorted list of task names.
        """
        resolved: set = set()
        phases: List[List[str]] = []
        remaining = set(self._registry.keys())

        while remaining:
            ready: set = set()
            for name in remaining:
                deps = self._deps.get(name, [])
                # Only consider deps that are actually registered
                relevant = [d for d in deps if d in self._registry]
                if all(d in resolved for d in relevant):
                    ready.add(name)

            if not ready:
                # Circular dependency -- force remaining into final phase
                logger.warning(
                    "Circular or unresolvable dependencies detected, "
                    "forcing %d tasks into final phase: %s",
                    len(remaining),
                    sorted(remaining),
                )
                phases.append(sorted(remaining))
                break

            phases.append(sorted(ready))
            resolved.update(ready)
            remaining -= ready

        return phases

    def get_func(self, name: str) -> Callable:
        """Return the callable registered under *name*."""
        return self._registry[name]

    def get_dependencies(self, name: str) -> List[str]:
        """Return declared dependencies for *name*."""
        return list(self._deps.get(name, []))
