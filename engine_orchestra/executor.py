"""Parallel executor with dependency-aware phased execution."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

from .base import BaseEngine, EngineResult
from .plan import ExecutionPlan

logger = logging.getLogger(__name__)


class ParallelExecutor:
    """Execute tasks in parallel phases respecting dependency order.

    Tasks are grouped into phases via topological sort.  Within each
    phase, all tasks run concurrently using a ``ThreadPoolExecutor``.
    Results from earlier phases are available to later phases through a
    shared results dictionary.

    Args:
        max_workers: Maximum number of threads per phase.
    """

    def __init__(self, max_workers: int = 8) -> None:
        self.max_workers = max_workers
        self._execution_log: List[Dict[str, Any]] = []

    def execute(
        self,
        plan: ExecutionPlan,
        context: Dict[str, Any],
    ) -> Dict[str, EngineResult]:
        """Execute all tasks in *plan*, returning results keyed by task name.

        Each registered callable is invoked as ``func(context)`` and must
        return an ``EngineResult`` (or ``None``).  Tasks whose registered
        dependencies did not produce a result receive a degraded result
        instead of being executed.

        Args:
            plan: An ``ExecutionPlan`` with registered tasks.
            context: Shared context dictionary passed to every task.

        Returns:
            Dictionary mapping task name to its ``EngineResult``.
        """
        phases = plan.build_phases()
        results: Dict[str, EngineResult] = {}
        all_names = set(plan.task_names)
        start_total = time.perf_counter()

        for phase_idx, phase_names in enumerate(phases):
            phase_start = time.perf_counter()

            ready: List[str] = []
            skipped: List[str] = []

            for name in phase_names:
                deps = plan.get_dependencies(name)
                missing = [
                    d for d in deps
                    if d in all_names and d not in results
                ]
                if missing:
                    logger.warning(
                        "[EXECUTOR] Skipping %s: missing dependencies %s",
                        name, missing,
                    )
                    degraded = _create_degraded_result(name, missing)
                    results[name] = degraded
                    skipped.append(name)
                else:
                    ready.append(name)

            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                future_map = {}
                for name in ready:
                    func = plan.get_func(name)
                    future = pool.submit(_safe_call, name, func, context)
                    future_map[future] = name

                for future in as_completed(future_map):
                    name = future_map[future]
                    try:
                        result = future.result()
                        if result is not None:
                            results[name] = result
                            self._log(name, "SUCCESS", time.perf_counter() - start_total)
                    except Exception as exc:
                        logger.error(
                            "[EXECUTOR] Phase %d task %s failed: %s",
                            phase_idx, name, exc,
                        )
                        self._log(name, "ERROR", time.perf_counter() - start_total)

            phase_ms = (time.perf_counter() - phase_start) * 1000
            logger.info(
                "[EXECUTOR] Phase %d: %d ready, %d skipped, %.1fms",
                phase_idx, len(ready), len(skipped), phase_ms,
            )

        total_ms = (time.perf_counter() - start_total) * 1000
        logger.info(
            "[EXECUTOR] Completed %d tasks in %.1fms across %d phases",
            len(results), total_ms, len(phases),
        )
        return results

    def execute_engines(
        self,
        engines: List[BaseEngine],
        context: Dict[str, Any],
    ) -> Dict[str, EngineResult]:
        """Build a plan from *BaseEngine* instances and execute it.

        This is a convenience wrapper: it registers each engine in a
        fresh ``ExecutionPlan`` using the engine's ``NAME`` and
        ``DEPENDENCIES``, then delegates to ``execute()``.

        Args:
            engines: List of ``BaseEngine`` subclass instances.
            context: Shared context dictionary.

        Returns:
            Dictionary mapping engine name to its ``EngineResult``.
        """
        plan = ExecutionPlan()
        for engine in engines:
            name = engine.get_name()
            plan.register(name, engine.safe_analyze, dependencies=list(engine.DEPENDENCIES))
        return self.execute(plan, context)

    @property
    def execution_log(self) -> List[Dict[str, Any]]:
        """Return a copy of the execution log."""
        return list(self._execution_log)

    def _log(self, name: str, status: str, elapsed: float) -> None:
        self._execution_log.append({
            "task": name,
            "status": status,
            "elapsed_s": elapsed,
            "timestamp": time.time(),
        })


class Orchestra:
    """High-level API for dependency-aware parallel task execution.

    Supports both function-based and class-based (``BaseEngine``) tasks.

    Example::

        orch = Orchestra(max_workers=4)
        orch.register("fetch", fetch_data)
        orch.register("clean", clean_data, dependencies=["fetch"])
        orch.add_engine(MyAggregatorEngine())

        results = orch.execute({"url": "https://example.com"})

    Args:
        max_workers: Maximum threads per phase.
    """

    def __init__(self, max_workers: int = 8) -> None:
        self.max_workers = max_workers
        self._plan = ExecutionPlan()
        self._engines: List[BaseEngine] = []

    def register(
        self,
        name: str,
        func: Callable,
        dependencies: Optional[List[str]] = None,
    ) -> None:
        """Register a function-based task.

        The function signature must be ``func(context: dict) -> EngineResult``.

        Args:
            name: Unique task identifier.
            func: Callable that accepts a context dict and returns an EngineResult.
            dependencies: Task names that must complete before this one.
        """
        self._plan.register(name, func, dependencies=dependencies)

    def add_engine(self, engine: BaseEngine) -> None:
        """Register a class-based engine.

        The engine's ``NAME`` is used as the task identifier, and its
        ``DEPENDENCIES`` list declares ordering constraints.
        """
        self._engines.append(engine)

    def execute(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, EngineResult]:
        """Run all registered tasks and engines, returning results by name.

        Args:
            context: Shared context dictionary. Defaults to an empty dict.

        Returns:
            Dictionary mapping task/engine name to its ``EngineResult``.
        """
        ctx = context if context is not None else {}
        executor = ParallelExecutor(max_workers=self.max_workers)

        # Merge class-based engines into the plan
        plan = ExecutionPlan()

        # Copy function-based registrations
        for name in self._plan.task_names:
            plan.register(
                name,
                self._plan.get_func(name),
                dependencies=self._plan.get_dependencies(name),
            )

        # Add class-based engines
        for engine in self._engines:
            ename = engine.get_name()
            plan.register(ename, engine.safe_analyze, dependencies=list(engine.DEPENDENCIES))

        return executor.execute(plan, ctx)


def _safe_call(
    name: str,
    func: Callable,
    context: Dict[str, Any],
) -> Optional[EngineResult]:
    """Call *func(context)* and catch exceptions."""
    try:
        return func(context)
    except Exception as exc:
        logger.error("[TASK] %s crashed: %s", name, exc)
        return None


def _create_degraded_result(
    name: str,
    missing_deps: List[str],
) -> EngineResult:
    """Create a minimal result for a task whose dependencies were not met."""
    return EngineResult(
        engine_name=name,
        confidence=0.0,
        scores={},
        insights={"dependency_failure": f"Missing: {', '.join(missing_deps)}"},
        dependencies_met=False,
    )
