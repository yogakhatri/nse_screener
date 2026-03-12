"""
Compatibility wrapper for historical command usage.
Delegates to scripts/run_engine.py in live mode.
"""
from __future__ import annotations

from scripts.run_engine import main


if __name__ == "__main__":
    main()
