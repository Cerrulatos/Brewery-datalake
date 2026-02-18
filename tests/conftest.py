from pathlib import Path
import pytest

@pytest.fixture
def isolated_env(tmp_path: Path, monkeypatch):
    datalake = tmp_path / "datalake"
    logs = tmp_path / "logs"

    datalake.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("DATALAKE_PATH", str(datalake))
    monkeypatch.setenv("LOG_FOLDER", str(logs))

    return {"tmp": tmp_path, "datalake": datalake, "logs": logs}
