from __future__ import annotations

import json
import sys
from pathlib import Path

BACKEND_PYTHON_TRAINING = Path(__file__).resolve().parents[1]
if str(BACKEND_PYTHON_TRAINING) not in sys.path:
    sys.path.insert(0, str(BACKEND_PYTHON_TRAINING))

import training_worker  # noqa: E402


def test_read_control_defaults_to_running_when_missing_or_invalid(tmp_path: Path) -> None:
    missing = tmp_path / "control.json"
    assert training_worker.read_control(missing) == "running"

    missing.write_text("{not-valid-json", encoding="utf-8")
    assert training_worker.read_control(missing) == "running"

    missing.write_text(json.dumps({"command": "paused"}), encoding="utf-8")
    assert training_worker.read_control(missing) == "paused"
