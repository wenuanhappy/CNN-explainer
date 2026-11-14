from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
import torch

WORKER_DIR = Path(__file__).resolve().parents[1]
if str(WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(WORKER_DIR))

import training_worker  # noqa: E402


def _capture_emitted(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    monkeypatch.setattr(training_worker, "emit", lambda payload: events.append(dict(payload)))
    return events


def test_read_control_defaults_and_parses_command(tmp_path: Path) -> None:
    missing = tmp_path / "missing-control.json"
    assert training_worker.read_control(missing) == "running"

    control_file = tmp_path / "control.json"
    control_file.write_text(json.dumps({"command": "paused"}), encoding="utf-8")
    assert training_worker.read_control(control_file) == "paused"


def test_select_device_respects_cuda_availability(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    assert training_worker.select_device().type == "cuda"

    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    assert training_worker.select_device().type == "cpu"


def test_wait_if_paused_emits_paused_once_and_running_on_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = _capture_emitted(monkeypatch)
    control_file = tmp_path / "control.json"
    states = iter(["paused", "paused", "running"])
    monkeypatch.setattr(training_worker, "read_control", lambda _: next(states))
    monkeypatch.setattr(training_worker.time, "sleep", lambda _: None)

    result = training_worker.wait_if_paused(control_file, job_id="job-pause-1", device_type="cpu", poll_interval=0.0)
    assert result == "running"
    assert [e["status"] for e in events] == ["paused", "running"]
    assert all(e["jobId"] == "job-pause-1" and e["device"] == "cpu" for e in events)


def test_wait_if_paused_transitions_to_stopped_without_emitting_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = _capture_emitted(monkeypatch)
    control_file = tmp_path / "control.json"
    states = iter(["paused", "stopped"])
    monkeypatch.setattr(training_worker, "read_control", lambda _: next(states))
    monkeypatch.setattr(training_worker.time, "sleep", lambda _: None)

    result = training_worker.wait_if_paused(control_file, job_id="job-stop-1", device_type="cuda", poll_interval=0.0)
    assert result == "stopped"
    assert [e["status"] for e in events] == ["paused"]


def test_cpu_small_scale_training_reports_device_and_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = _capture_emitted(monkeypatch)
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)

    control_file = tmp_path / "control.json"
    control_file.write_text(json.dumps({"command": "running"}), encoding="utf-8")
    checkpoint_file = tmp_path / "checkpoint.pt"
    request_file = tmp_path / "request.json"
    request_payload = {
        "jobId": "job-cpu-train",
        "datasetRoot": str(tmp_path / "datasets"),
        "controlFile": str(control_file),
        "datasetId": "house-price-regression",
        "split": {"train": 0.7, "val": 0.15, "test": 0.15},
        "layers": [
            {"id": 1, "type": "input", "enabled": True, "params": {"inputKind": "table", "featureCount": 5}},
            {"id": 2, "type": "dense", "enabled": True, "params": {"units": 8, "activation": "relu"}},
            {"id": 3, "type": "output", "enabled": True, "params": {"units": 1, "activation": "none"}},
        ],
        "connections": [],
        "config": {
            "batchSize": 64,
            "totalEpochs": 2,
            "learningRate": 0.01,
            "optimizer": "Adam",
            "scheduler": "none",
            "lossFunction": "mse",
        },
        "checkpointFile": str(checkpoint_file),
        "modelSignature": "sig-cpu-1",
    }
    request_file.write_text(json.dumps(request_payload), encoding="utf-8")

    exit_code = training_worker.run_cli(["--request", str(request_file)])
    assert exit_code == training_worker.EXIT_OK
    assert checkpoint_file.exists()

    control_events = [e for e in events if e.get("type") == "control"]
    assert control_events[0]["status"] == "running"
    assert control_events[0]["device"] == "cpu"
    assert control_events[-1]["status"] == "completed"
    assert control_events[-1]["device"] == "cpu"

    metric_events = [e for e in events if e.get("type") == "metric"]
    assert len(metric_events) == 2
    assert all(m.get("device") == "cpu" for m in metric_events)


def test_mid_epoch_stop_aborts_immediately_without_checkpoint_or_completed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = _capture_emitted(monkeypatch)
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)

    control_file = tmp_path / "control.json"
    checkpoint_file = tmp_path / "checkpoint.pt"
    # Poll 1 (epoch check) -> running, Poll 2 (batch 1) -> running, Poll 3 (batch 2) -> stopped
    commands = iter(["running", "running", "stopped"])
    monkeypatch.setattr(training_worker, "read_control", lambda _: next(commands, "stopped"))

    request_payload = {
        "jobId": "job-mid-stop",
        "datasetRoot": str(tmp_path / "datasets"),
        "controlFile": str(control_file),
        "datasetId": "house-price-regression",
        "split": {"train": 0.7, "val": 0.15, "test": 0.15},
        "layers": [
            {"id": 1, "type": "input", "enabled": True, "params": {"inputKind": "table", "featureCount": 5}},
            {"id": 2, "type": "output", "enabled": True, "params": {"units": 1, "activation": "none"}},
        ],
        "config": {
            "batchSize": 32,
            "totalEpochs": 3,
            "learningRate": 0.01,
            "optimizer": "SGD",
            "lossFunction": "mse",
        },
        "checkpointFile": str(checkpoint_file),
    }
    training_worker.train(request_payload)

    assert not checkpoint_file.exists()
    control_statuses = [e["status"] for e in events if e.get("type") == "control"]
    assert control_statuses == ["running", "stopped"]
    assert not any(e.get("type") == "test_result" for e in events)


def test_cuda_oom_and_generic_errors_emit_structured_codes_and_exit_codes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = _capture_emitted(monkeypatch)
    request_file = tmp_path / "request.json"
    request_file.write_text(json.dumps({"jobId": "job-oom-99"}), encoding="utf-8")

    def raise_oom(_: dict[str, Any]) -> None:
        raise RuntimeError("CUDA out of memory. Tried to allocate 4.00 GiB on GPU 0.")

    monkeypatch.setattr(training_worker, "train", raise_oom)
    code_oom = training_worker.run_cli(["--request", str(request_file)])
    assert code_oom == training_worker.EXIT_CUDA_OOM
    assert events[-1]["type"] == "error"
    assert events[-1]["jobId"] == "job-oom-99"
    assert events[-1]["errorCode"] == "CUDA_OOM"
    assert "batchSize" in events[-1]["message"]

    def raise_generic(_: dict[str, Any]) -> None:
        raise ValueError("Unsupported layer parameter")

    monkeypatch.setattr(training_worker, "train", raise_generic)
    code_err = training_worker.run_cli(["--request", str(request_file)])
    assert code_err == training_worker.EXIT_RUNTIME_ERROR
    assert events[-1]["errorCode"] == "WORKER_RUNTIME_ERROR"
    assert "Unsupported layer parameter" in events[-1]["message"]
