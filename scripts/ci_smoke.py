#!/usr/bin/env python3
"""Simple CI smoke test to verify Python runs in the environment."""

import sys

print("CI smoke test: Python", sys.version.split()[0])
print("Checking repository layout...")

expected = ["src", "dbt", "airflow", "data"]
missing = [p for p in expected if not __import__('os').path.exists(p)]
if missing:
    print("Warning: expected paths missing:", ", ".join(missing))
else:
    print("Repository layout looks present.")

print("OK")
