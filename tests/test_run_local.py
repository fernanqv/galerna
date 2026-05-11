import csv
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

    status_rows = read_status_rows(output_dir / "0000" / "galerna.status")
    assert [row["status"] for row in status_rows] == ["STARTED", "DONE"]


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

    status_rows = read_status_rows(output_dir / "0000" / "galerna.status")
    assert [row["status"] for row in status_rows] == ["STARTED", "FAILED"]
    assert status_rows[-1]["message"] == "exit_code=7"
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
        ("0000", "STARTED"),
        ("0000", "DONE"),
        ("0001", "STARTED"),
        ("0001", "DONE"),
    ]
