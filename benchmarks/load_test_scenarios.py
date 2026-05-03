"""Locust load test scenarios entrypoint.

This module mirrors the existing `locustfile.py` so the repository matches
the assignment structure while keeping the current benchmark configuration.
Run with:

    locust -f benchmarks/load_test_scenarios.py --host http://localhost:8001
"""

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
BENCHMARK_DIR = Path(__file__).resolve().parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(BENCHMARK_DIR) not in sys.path:
    sys.path.insert(0, str(BENCHMARK_DIR))

from locustfile import *  # noqa: F401,F403
