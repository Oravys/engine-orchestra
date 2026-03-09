"""Basic usage example for engine-orchestra.

Demonstrates both function-based and class-based task registration,
dependency declaration, and parallel execution.
"""

import time
import random
from engine_orchestra import Orchestra, BaseEngine, EngineResult


# ---------------------------------------------------------------------------
# Function-based tasks
# ---------------------------------------------------------------------------

def fetch_data(context: dict) -> EngineResult:
    """Simulate fetching data from an external source."""
    time.sleep(0.1)  # simulate I/O
    context["raw_data"] = list(range(100))
    return EngineResult(
        engine_name="fetch_data",
        confidence=1.0,
        scores={"records": 100},
        insights={"source": "simulated"},
    )


def validate_data(context: dict) -> EngineResult:
    """Validate the fetched data."""
    raw = context.get("raw_data", [])
    valid = [x for x in raw if x >= 0]
    context["valid_data"] = valid
    return EngineResult(
        engine_name="validate_data",
        confidence=0.99,
        scores={"valid_count": len(valid), "rejected": len(raw) - len(valid)},
    )


def compute_statistics(context: dict) -> EngineResult:
    """Compute summary statistics on validated data."""
    data = context.get("valid_data", [])
    if not data:
        return EngineResult(engine_name="compute_statistics", confidence=0.0)
    mean_val = sum(data) / len(data)
    return EngineResult(
        engine_name="compute_statistics",
        confidence=0.95,
        scores={"mean": mean_val, "count": len(data)},
        insights={"status": "complete"},
    )


# ---------------------------------------------------------------------------
# Class-based engine
# ---------------------------------------------------------------------------

class AnomalyDetector(BaseEngine):
    """Detect anomalies in validated data using a simple z-score check."""

    NAME = "anomaly_detector"
    DEPENDENCIES = ["validate_data"]

    def analyze(self, context: dict) -> EngineResult:
        data = context.get("valid_data", [])
        if len(data) < 2:
            return EngineResult(engine_name=self.NAME, confidence=0.0)

        mean = sum(data) / len(data)
        std = (sum((x - mean) ** 2 for x in data) / len(data)) ** 0.5
        threshold = mean + 2 * std if std > 0 else mean + 1
        anomalies = [x for x in data if x > threshold]

        return EngineResult(
            engine_name=self.NAME,
            confidence=0.85,
            scores={"anomaly_count": len(anomalies), "threshold": threshold},
            insights={"method": "z-score", "sigma": 2},
        )


class TrendAnalyzer(BaseEngine):
    """Analyze trends -- depends on both statistics and anomaly detection."""

    NAME = "trend_analyzer"
    DEPENDENCIES = ["compute_statistics", "anomaly_detector"]

    def analyze(self, context: dict) -> EngineResult:
        time.sleep(0.05)  # simulate work
        direction = random.choice(["up", "down", "stable"])
        return EngineResult(
            engine_name=self.NAME,
            confidence=0.72,
            scores={"trend_strength": random.uniform(0.3, 0.9)},
            insights={"direction": direction},
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    orch = Orchestra(max_workers=4)

    # Register function-based tasks with dependencies
    orch.register("fetch_data", fetch_data)
    orch.register("validate_data", validate_data, dependencies=["fetch_data"])
    orch.register("compute_statistics", compute_statistics, dependencies=["validate_data"])

    # Register class-based engines
    orch.add_engine(AnomalyDetector())
    orch.add_engine(TrendAnalyzer())

    # Execute everything
    context = {"config": {"verbose": True}}
    results = orch.execute(context)

    # Print results
    print("=" * 60)
    print("  engine-orchestra results")
    print("=" * 60)
    for name, result in sorted(results.items()):
        deps_status = "OK" if result.dependencies_met else "DEGRADED"
        print(f"\n  [{deps_status}] {name}")
        print(f"    confidence : {result.confidence:.2f}")
        if result.scores:
            print(f"    scores     : {result.scores}")
        if result.insights:
            print(f"    insights   : {result.insights}")

    print(f"\nTotal tasks executed: {len(results)}")


if __name__ == "__main__":
    main()
