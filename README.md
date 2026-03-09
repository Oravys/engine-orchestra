# engine-orchestra

A dependency-aware parallel task execution framework for Python.

Organize your processing pipeline as a collection of tasks (engines) with
explicit dependencies. engine-orchestra topologically sorts them into
parallel phases and executes each phase concurrently using
`concurrent.futures.ThreadPoolExecutor`.

**Zero external dependencies** -- uses only the Python standard library.

## Features

- **Topological sort** into parallel execution phases
- **Dependency validation** with degraded results for missing dependencies
- **Circular dependency detection** with automatic fallback
- **Mixed API**: register plain functions or class-based engines
- **Shared context**: tasks communicate through a plain dictionary
- **Thread-safe execution** with configurable worker count
- **Detailed execution logging** for diagnostics

## Installation

```bash
pip install engine-orchestra
```

Or install from source:

```bash
git clone https://github.com/Oravys/engine-orchestra.git
cd engine-orchestra
pip install -e .
```

## Quick Start

### Function-based tasks

```python
from engine_orchestra import Orchestra, EngineResult

def fetch(ctx):
    ctx["data"] = [1, 2, 3]
    return EngineResult(engine_name="fetch", confidence=1.0)

def process(ctx):
    total = sum(ctx["data"])
    return EngineResult(engine_name="process", confidence=0.95,
                        scores={"total": total})

orch = Orchestra(max_workers=4)
orch.register("fetch", fetch)
orch.register("process", process, dependencies=["fetch"])

results = orch.execute()
print(results["process"].scores)  # {"total": 6}
```

### Class-based engines

```python
from engine_orchestra import Orchestra, BaseEngine, EngineResult

class Validator(BaseEngine):
    NAME = "validator"
    DEPENDENCIES = ["fetch"]

    def analyze(self, context):
        data = context.get("data", [])
        valid = [x for x in data if x > 0]
        return EngineResult(
            engine_name=self.NAME,
            confidence=len(valid) / max(len(data), 1),
            scores={"valid": len(valid)},
        )

orch = Orchestra()
orch.register("fetch", fetch)
orch.add_engine(Validator())
results = orch.execute()
```

### Mixed registration

Function-based and class-based tasks can depend on each other freely:

```python
orch = Orchestra(max_workers=8)
orch.register("ingest", ingest_func)
orch.add_engine(TransformEngine())           # depends on "ingest"
orch.register("export", export_func, dependencies=["TransformEngine"])
results = orch.execute({"path": "/data"})
```

## How It Works

1. **Registration** -- tasks are registered with a name, a callable, and
   an optional list of dependency names.

2. **Phase building** -- `ExecutionPlan.build_phases()` performs a
   topological sort, grouping tasks into phases. Tasks in the same phase
   have no inter-dependencies and run concurrently.

3. **Execution** -- `ParallelExecutor` iterates through phases
   sequentially. Within each phase, a `ThreadPoolExecutor` runs all
   ready tasks in parallel.

4. **Dependency validation** -- before executing a task, the executor
   checks that all declared dependencies produced a result. If a
   dependency is missing (crashed or returned None), the dependent task
   receives a degraded `EngineResult` with `dependencies_met=False`
   instead of running.

5. **Circular dependency handling** -- if the topological sort detects a
   cycle (no tasks can be resolved), all remaining tasks are forced into
   a final phase with a warning.

## API Reference

### `Orchestra(max_workers=8)`

High-level API combining registration and execution.

| Method | Description |
|--------|-------------|
| `register(name, func, dependencies=[])` | Register a function-based task |
| `add_engine(engine)` | Register a `BaseEngine` subclass instance |
| `execute(context=None)` | Run all tasks, returns `dict[str, EngineResult]` |

### `BaseEngine`

Abstract base class for class-based engines.

| Attribute / Method | Description |
|--------------------|-------------|
| `NAME` | String identifier (falls back to class name) |
| `DEPENDENCIES` | List of dependency task names |
| `analyze(context) -> EngineResult` | Abstract -- implement your logic here |
| `safe_analyze(context)` | Wraps `analyze()` with exception handling |

### `EngineResult`

Dataclass returned by every task.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `engine_name` | `str` | required | Task identifier |
| `confidence` | `float` | `0.0` | Result confidence [0, 1] |
| `scores` | `dict[str, float]` | `{}` | Numeric scores |
| `insights` | `dict[str, Any]` | `{}` | Arbitrary metadata |
| `dependencies_met` | `bool` | `True` | False if deps were missing |

### `ExecutionPlan`

Low-level plan builder (used internally by `Orchestra`).

| Method | Description |
|--------|-------------|
| `register(name, func, dependencies=[])` | Add a task |
| `build_phases()` | Topological sort into `list[list[str]]` |
| `task_names` | Property: all registered names |

### `ParallelExecutor(max_workers=8)`

Low-level executor (used internally by `Orchestra`).

| Method | Description |
|--------|-------------|
| `execute(plan, context)` | Execute an `ExecutionPlan` |
| `execute_engines(engines, context)` | Execute a list of `BaseEngine` instances |
| `execution_log` | Property: list of execution log entries |

## Running Tests

```bash
python -m pytest tests/
```

Or with unittest:

```bash
python -m unittest discover tests/
```

## License

MIT -- see [LICENSE](LICENSE) for details.
