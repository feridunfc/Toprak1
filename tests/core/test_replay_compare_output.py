import json

from scripts import replay_compare


def test_replay_compare_writes_output_artifact(tmp_path, capsys):
    output = tmp_path / "docs" / "dashboard" / "artifacts" / "latest_replay.json"

    code = replay_compare.main([
        "--json",
        "--output",
        str(output),
    ])

    printed = json.loads(capsys.readouterr().out)
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert code == 0
    assert output.exists()
    assert payload == printed
    assert payload["mode"] == "read-only"
    assert payload["source"] == "replay_compare"
    assert payload["replay_status"] == "PASS"
    assert payload["replay_clean"] is True
    assert payload["deterministic_replay_ok"] is True


def test_replay_compare_writes_failed_output_artifact(tmp_path, capsys):
    output = tmp_path / "latest_replay.json"

    code = replay_compare.main([
        "--json",
        "--mismatch",
        "--output",
        str(output),
    ])

    printed = json.loads(capsys.readouterr().out)
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert code == 1
    assert payload == printed
    assert payload["replay_status"] == "FAIL"
    assert payload["mismatch"] is True
    assert payload["reason"] == "runtime_replay_mismatch"
