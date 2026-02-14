"""
Chapter 08 - Python entrypoint

- Runs a tiny local pipeline instrumented with metrics.
- For tests, run: pytest -q
"""
from metrics_example import run

if __name__ == "__main__":
    run()
