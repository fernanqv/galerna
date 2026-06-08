import csv
import os
import subprocess

import pytest

from galerna import Galerna


def read_manifest(output_dir):
    manifest_path = output_dir / ".galerna" / "cases.tsv"
    with manifest_path.open(newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def read_status_rows(status_file):
    with status_file.open(newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def test_run_local_executes_cases_sequentially(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2]},
        command=(
            "python -c 'from pathlib import Path; "
            'Path("result_{{case_id}}.txt").write_text("{{station}}")\''
        ),
    )

    wrapper.run_cases()

    assert (output_dir / "0000" / "result_0000.txt").read_text() == "1"
    assert (output_dir / "0001" / "result_0001.txt").read_text() == "2"
    assert (output_dir / "0000" / ".galerna.done").is_file()
    assert (output_dir / "0001" / ".galerna.done").is_file()

    status_rows = read_status_rows(
        output_dir / ".galerna" / "status" / "status_0000.tsv"
    )
    assert [row["status"] for row in status_rows] == ["BUILT", "STARTED", "DONE"]
    assert {row["case_id"] for row in status_rows} == {"0000"}


def test_run_local_directory_layout_writes_stdout_and_stderr_logs(tmp_path, capsys):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1]},
        command=(
            "python -c 'import sys; "
            'print("stdout from {{case_id}}"); '
            'print("stderr from {{case_id}}", file=sys.stderr)\''
        ),
        log_console=False,
    )

    wrapper.run_cases()

    assert (output_dir / "0000" / "galerna.out").read_text() == "stdout from 0000\n"
    assert (output_dir / "0000" / "galerna.err").read_text() == "stderr from 0000\n"
    captured = capsys.readouterr()
    assert "stdout from 0000" not in captured.out
    assert "stderr from 0000" not in captured.err


def test_run_local_can_discard_stdout_without_creating_log_file(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1]},
        command='python -c \'print("discard me")\'',
        logs={"stdout": "discard"},
    )

    wrapper.run_cases()

    rows = read_manifest(output_dir)
    assert rows[0]["stdout_log"] == os.devnull
    assert not (output_dir / "0000" / "galerna.out").exists()
    assert (output_dir / "0000" / "galerna.err").is_file()


def test_run_local_cases_subset_only_executes_selected_cases(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2, 3, 4]},
        command=(
            "python -c 'from pathlib import Path; "
            'Path("result_{{case_id}}.txt").write_text("{{station}}")\''
        ),
    )

    wrapper.run_cases(cases=[1, 3])

    rows = read_manifest(output_dir)
    assert [row["case_id"] for row in rows] == ["0000", "0001", "0002", "0003"]
    assert not (output_dir / "0000").exists()
    assert (output_dir / "0001" / "result_0001.txt").is_file()
    assert not (output_dir / "0002").exists()
    assert (output_dir / "0003" / "result_0003.txt").is_file()


def test_run_local_failure_writes_failed_status_without_done_file(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1]},
        command='python -c \'import sys; print("boom"); sys.exit(7)\'',
    )

    with pytest.raises(subprocess.CalledProcessError):
        wrapper.run_cases()

    status_rows = read_status_rows(
        output_dir / ".galerna" / "status" / "status_0000.tsv"
    )
    assert [row["status"] for row in status_rows] == ["BUILT", "STARTED", "FAILED"]
    assert status_rows[-1]["message"] == "exit_code=7"
    assert (output_dir / "0000" / "galerna.out").read_text() == "boom\n"
    assert not (output_dir / "0000" / ".galerna.done").exists()


def test_run_local_shared_layout_uses_group_status_and_done(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        cases={"layout": "shared"},
        variable_parameters={"station": [1, 2]},
        command=(
            "python -c 'from pathlib import Path; "
            'Path("result_{{case_id}}.txt").write_text("{{station}}")\''
        ),
    )

    wrapper.run_cases()

    assert (output_dir / "result_0000.txt").read_text() == "1"
    assert (output_dir / "result_0001.txt").read_text() == "2"
    assert (output_dir / ".galerna" / "done" / "cases.done").is_file()

    status_rows = read_status_rows(
        output_dir / ".galerna" / "status" / "status_cases.tsv"
    )
    assert [(row["case_id"], row["status"]) for row in status_rows] == [
        ("0000", "BUILT"),
        ("0001", "BUILT"),
        ("0000", "STARTED"),
        ("0000", "DONE"),
        ("0001", "STARTED"),
        ("0001", "DONE"),
    ]


def test_run_local_shared_layout_writes_logs_under_galerna(tmp_path, capsys):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        cases={"layout": "shared"},
        variable_parameters={"station": [1]},
        command=(
            "python -c 'import sys; "
            'print("shared stdout {{case_id}}"); '
            'print("shared stderr {{case_id}}", file=sys.stderr)\''
        ),
        log_console=False,
    )

    wrapper.run_cases()

    logs_dir = output_dir / ".galerna" / "logs"
    assert (logs_dir / "0000.out").read_text() == "shared stdout 0000\n"
    assert (logs_dir / "0000.err").read_text() == "shared stderr 0000\n"
    assert not (output_dir / "galerna.out").exists()
    assert not (output_dir / "galerna.err").exists()
    captured = capsys.readouterr()
    assert "shared stdout 0000" not in captured.out
    assert "shared stderr 0000" not in captured.err
