#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from engine.cards import validate_metric_direction_map
from engine.config import configured_core_cards, configured_template_codes, validate_runtime_config
from scripts.load_data import validate_loader_support


def main() -> None:
    validate_runtime_config()
    validate_metric_direction_map()
    validate_loader_support()

    payload = {
        "status": "ok",
        "templates": list(configured_template_codes()),
        "core_cards": list(configured_core_cards()),
    }
    print("Config validation passed.")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
