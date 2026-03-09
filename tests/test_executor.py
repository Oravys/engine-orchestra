"""Tests for engine-orchestra core functionality."""

import unittest
import logging
from engine_orchestra import Orchestra, BaseEngine, EngineResult, ExecutionPlan, ParallelExecutor


class TestExecutionPlan(unittest.TestCase):
    """Tests for the ExecutionPlan topological sort."""

    def test_no_deps_single_phase(self):
        plan = ExecutionPlan()
        plan.register("a", lambda ctx: None)
        plan.register("b", lambda ctx: None)
        plan.register("c", lambda ctx: None)
        phases = plan.build_phases()
        self.assertEqual(len(phases), 1)
        self.assertEqual(sorted(phases[0]), ["a", "b", "c"])

    def test_linear_chain(self):
        plan = ExecutionPlan()
        plan.register("first", lambda ctx: None)
        plan.register("second", lambda ctx: None, dependencies=["first"])
        plan.register("third", lambda ctx: None, dependencies=["second"])
        phases = plan.build_phases()
        self.assertEqual(len(phases), 3)
        self.assertEqual(phases[0], ["first"])
        self.assertEqual(phases[1], ["second"])
        self.assertEqual(phases[2], ["third"])

    def test_diamond_dependency(self):
        plan = ExecutionPlan()
        plan.register("root", lambda ctx: None)
        plan.register("left", lambda ctx: None, dependencies=["root"])
        plan.register("right", lambda ctx: None, dependencies=["root"])
        plan.register("join", lambda ctx: None, dependencies=["left", "right"])
        phases = plan.build_phases()
        self.assertEqual(len(phases), 3)
        self.assertEqual(phases[0], ["root"])
        self.assertIn("left", phases[1])
        self.assertIn("right", phases[1])
        self.assertEqual(phases[2], ["join"])

    def test_circular_dependency_forced(self):
        plan = ExecutionPlan()
        plan.register("a", lambda ctx: None, dependencies=["b"])
        plan.register("b", lambda ctx: None, dependencies=["a"])
        with self.assertLogs(level=logging.WARNING):
            phases = plan.build_phases()
        # Both should be forced into a single phase
        self.assertEqual(len(phases), 1)
        self.assertEqual(sorted(phases[0]), ["a", "b"])

    def test_unregistered_dep_ignored(self):
        plan = ExecutionPlan()
        plan.register("task", lambda ctx: None, dependencies=["nonexistent"])
        phases = plan.build_phases()
        self.assertEqual(len(phases), 1)
        self.assertEqual(phases[0], ["task"])

    def test_empty_plan(self):
        plan = ExecutionPlan()
        phases = plan.build_phases()
        self.assertEqual(phases, [])


class TestParallelExecutor(unittest.TestCase):
    """Tests for the ParallelExecutor."""

    def _make_func(self, name, value=1.0):
        def func(ctx):
            return EngineResult(engine_name=name, confidence=value)
        return func

    def test_basic_execution(self):
        plan = ExecutionPlan()
        plan.register("a", self._make_func("a", 0.9))
        plan.register("b", self._make_func("b", 0.8))
        executor = ParallelExecutor(max_workers=2)
        results = executor.execute(plan, {})
        self.assertEqual(len(results), 2)
        self.assertAlmostEqual(results["a"].confidence, 0.9)
        self.assertAlmostEqual(results["b"].confidence, 0.8)

    def test_dependency_ordering(self):
        order = []

        def step(name):
            def func(ctx):
                order.append(name)
                return EngineResult(engine_name=name, confidence=1.0)
            return func

        plan = ExecutionPlan()
        plan.register("first", step("first"))
        plan.register("second", step("second"), dependencies=["first"])
        executor = ParallelExecutor(max_workers=1)
        executor.execute(plan, {})
        self.assertEqual(order, ["first", "second"])

    def test_degraded_result_on_missing_dep(self):
        def failing(ctx):
            raise RuntimeError("boom")

        plan = ExecutionPlan()
        plan.register("broken", failing)
        plan.register("dependent", self._make_func("dependent"), dependencies=["broken"])
        executor = ParallelExecutor(max_workers=2)
        results = executor.execute(plan, {})
        # broken returned None (crash), so dependent gets degraded
        self.assertIn("dependent", results)
        self.assertFalse(results["dependent"].dependencies_met)

    def test_context_shared(self):
        def writer(ctx):
            ctx["shared_key"] = 42
            return EngineResult(engine_name="writer", confidence=1.0)

        def reader(ctx):
            val = ctx.get("shared_key", 0)
            return EngineResult(
                engine_name="reader",
                confidence=1.0,
                scores={"read_value": val},
            )

        plan = ExecutionPlan()
        plan.register("writer", writer)
        plan.register("reader", reader, dependencies=["writer"])
        executor = ParallelExecutor(max_workers=2)
        results = executor.execute(plan, {})
        self.assertEqual(results["reader"].scores["read_value"], 42)


class TestBaseEngine(unittest.TestCase):
    """Tests for the BaseEngine abstract class."""

    def test_subclass(self):
        class MyEngine(BaseEngine):
            NAME = "my_engine"
            DEPENDENCIES = ["other"]

            def analyze(self, context):
                return EngineResult(
                    engine_name=self.NAME,
                    confidence=0.75,
                    scores={"x": 1.0},
                )

        eng = MyEngine()
        self.assertEqual(eng.get_name(), "my_engine")
        result = eng.safe_analyze({})
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result.confidence, 0.75)

    def test_safe_analyze_catches_errors(self):
        class BadEngine(BaseEngine):
            NAME = "bad"

            def analyze(self, context):
                raise ValueError("intentional error")

        eng = BadEngine()
        result = eng.safe_analyze({})
        self.assertIsNone(result)

    def test_default_name_fallback(self):
        class UnnamedEngine(BaseEngine):
            def analyze(self, context):
                return None

        eng = UnnamedEngine()
        self.assertEqual(eng.get_name(), "UnnamedEngine")


class TestOrchestra(unittest.TestCase):
    """Tests for the high-level Orchestra API."""

    def test_mixed_registration(self):
        def func_task(ctx):
            return EngineResult(engine_name="func_task", confidence=0.5)

        class EngTask(BaseEngine):
            NAME = "eng_task"

            def analyze(self, context):
                return EngineResult(engine_name=self.NAME, confidence=0.7)

        orch = Orchestra(max_workers=2)
        orch.register("func_task", func_task)
        orch.add_engine(EngTask())
        results = orch.execute()
        self.assertIn("func_task", results)
        self.assertIn("eng_task", results)

    def test_empty_orchestra(self):
        orch = Orchestra()
        results = orch.execute()
        self.assertEqual(results, {})

    def test_cross_type_dependencies(self):
        """Function task depends on engine and vice versa."""
        class Producer(BaseEngine):
            NAME = "producer"

            def analyze(self, context):
                context["produced"] = True
                return EngineResult(engine_name=self.NAME, confidence=1.0)

        def consumer(ctx):
            val = ctx.get("produced", False)
            return EngineResult(
                engine_name="consumer",
                confidence=1.0 if val else 0.0,
            )

        orch = Orchestra(max_workers=2)
        orch.add_engine(Producer())
        orch.register("consumer", consumer, dependencies=["producer"])
        results = orch.execute()
        self.assertAlmostEqual(results["consumer"].confidence, 1.0)


class TestEngineResult(unittest.TestCase):
    """Tests for EngineResult dataclass."""

    def test_defaults(self):
        r = EngineResult(engine_name="test")
        self.assertEqual(r.confidence, 0.0)
        self.assertEqual(r.scores, {})
        self.assertEqual(r.insights, {})
        self.assertTrue(r.dependencies_met)

    def test_repr(self):
        r = EngineResult(engine_name="x", confidence=0.5)
        text = repr(r)
        self.assertIn("x", text)
        self.assertIn("0.5000", text)
        self.assertIn("OK", text)

    def test_degraded_repr(self):
        r = EngineResult(engine_name="y", dependencies_met=False)
        self.assertIn("DEGRADED", repr(r))


if __name__ == "__main__":
    unittest.main()
