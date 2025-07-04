#!/usr/bin/env python3
"""
Test runner script for CrawlGuard API tests.
"""

import subprocess
import sys


def run_tests(test_type="all"):
    """Run tests based on type."""

    if test_type == "integration":
        cmd = ["uv", "run", "pytest", "tests/integration/", "-v", "--tb=short"]
    elif test_type == "unit":
        cmd = ["uv", "run", "pytest", "tests/unit/", "-v", "--tb=short"]
    elif test_type == "all":
        cmd = ["uv", "run", "pytest", "tests/", "-v", "--tb=short"]
    else:
        print(f"Unknown test type: {test_type}")
        print("Available types: all, integration, unit")
        sys.exit(1)

    print(f"Running {test_type} tests...")
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    test_type = sys.argv[1] if len(sys.argv) > 1 else "all"
    exit_code = run_tests(test_type)
    sys.exit(exit_code)
