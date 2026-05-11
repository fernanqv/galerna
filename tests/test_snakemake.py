import csv
import shutil

import pytest

from galerna import Galerna


def read_status_rows(status_file):
    with status_file.open(newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def test_snakemake_cases_build_writes_generated_snakefile(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2]},
        command="echo {{station}}",
        run={"backend": "snakemake", "mode": "cases", "executor": "local"},
    )

    wrapper.build_cases()

    snakefile = output_dir / ".galerna" / "Snakefile"
    assert snakefile.is_file()
    content = snakefile.read_text()
    assert "MANIFEST =" in content
    assert "rule all:" in content
    assert "rule case_0:" in content
    assert "rule case_1:" in content
    assert "run_case(CASES[params.case_id])" in content


@pytest.mark.skipif(shutil.which("snakemake") is None, reason="snakemake not found")
def test_snakemake_cases_local_executor_runs_selected_cases(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2]},
        command=(
            "python -c 'from pathlib import Path; "
            'Path(\"result_{{case_id}}.txt\").write_text(\"{{station}}\")\''
        ),
        run={
            "backend": "snakemake",
            "mode": "cases",
            "executor": "local",
            "cores": 1,
        },
    )

    wrapper.run_cases(cases=[1])

    assert not (output_dir / "0000" / "result_0000.txt").exists()
    assert (output_dir / "0001" / "result_0001.txt").read_text() == "2"
    assert not (output_dir / "0000" / ".galerna.done").exists()
    assert (output_dir / "0001" / ".galerna.done").is_file()

    statuses = wrapper.status_cases()
    assert [row["status"] for row in statuses] == ["NOT_BUILT", "DONE"]
    status_rows = read_status_rows(output_dir / "0001" / "galerna.status")
    assert [row["status"] for row in status_rows] == ["BUILT", "STARTED", "DONE"]


@pytest.mark.skipif(shutil.which("snakemake") is None, reason="snakemake not found")
def test_snakemake_cases_shared_layout_uses_case_done_and_group_status(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        cases={"layout": "shared"},
        variable_parameters={"station": [1, 2]},
        command=(
            "python -c 'from pathlib import Path; "
            'Path(\"result_{{case_id}}.txt\").write_text(\"{{station}}\")\''
        ),
        run={
            "backend": "snakemake",
            "mode": "cases",
            "executor": "local",
            "cores": 1,
        },
    )

    wrapper.run_cases(cases=[0])

    assert (output_dir / "result_0000.txt").read_text() == "1"
    assert not (output_dir / "result_0001.txt").exists()
    assert (output_dir / ".galerna" / "done" / "0000.done").is_file()
    assert not (output_dir / ".galerna" / "done" / "cases.done").exists()

    status_rows = read_status_rows(
        output_dir / ".galerna" / "status" / "status_cases.tsv"
    )
    assert [(row["case_id"], row["status"]) for row in status_rows] == [
        ("0000", "BUILT"),
        ("0000", "STARTED"),
        ("0000", "DONE"),
    ]
