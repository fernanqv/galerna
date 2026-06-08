import csv
import shutil
import subprocess

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
    assert "rule case:" in content
    assert "rule case_0:" not in content
    assert "rule case_1:" not in content
    assert str(output_dir / "{case_id}" / ".galerna.done") in content
    assert "threads:" not in content
    assert "run_case(CASES[wildcards.case_id])" in content


def test_snakemake_bulk_build_writes_generated_snakefile(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2, 3, 4]},
        command="echo {{station}}",
        run={
            "backend": "snakemake",
            "mode": "bulk",
            "executor": "local",
            "cases_per_job": 2,
            "snakemake": {"rule": {"threads": 2}},
        },
    )

    wrapper.build_cases()

    snakefile = output_dir / ".galerna" / "Snakefile"
    content = snakefile.read_text()
    assert "rule bulk_0000:" in content
    assert "rule bulk_0001:" in content
    assert "threads: 2" in content
    assert "run_bulk(params.case_ids, output[0], threads)" in content
    assert str(output_dir / ".galerna" / "done" / "bulk_0000.done") in content


def test_snakemake_rules_include_threads_and_slurm_resources(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1]},
        command="echo {{station}}",
        run={
            "backend": "snakemake",
            "mode": "cases",
            "executor": "slurm",
            "snakemake": {
                "rule": {
                    "threads": 4,
                    "resources": {
                        "mem_mb_per_cpu": 1000,
                        "runtime": 30,
                        "slurm_partition": "meteo_long",
                    },
                }
            },
        },
    )

    wrapper.build_cases()

    content = (output_dir / ".galerna" / "Snakefile").read_text()
    assert "threads: 4" in content
    assert (
        "resources: mem_mb_per_cpu=1000, runtime=30, "
        "slurm_partition='meteo_long'"
    ) in content


def test_snakemake_slurm_command_uses_executor_jobs_and_cli_args(
    tmp_path, monkeypatch
):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2, 3, 4]},
        command="echo {{station}}",
        run={
            "backend": "snakemake",
            "mode": "bulk",
            "executor": "slurm",
            "cases_per_job": 2,
            "snakemake": {
                "rule": {
                    "threads": 16,
                    "resources": {"runtime": 120, "mem_mb_per_cpu": 4000},
                },
                "cli": {
                    "jobs": 20,
                    "keep-going": True,
                    "default-resources": {"disk_mb": 2000},
                },
            },
        },
    )
    commands = []

    def capture_run(command, check):
        commands.append(command)

    monkeypatch.setattr(subprocess, "run", capture_run)

    wrapper.run_cases(cases=[0, 1])

    command = commands[0]
    assert command[:6] == [
        "snakemake",
        "--snakefile",
        str(output_dir / ".galerna" / "Snakefile"),
        "--rerun-incomplete",
        "--executor",
        "slurm",
    ]
    assert "--jobs" in command
    assert command[command.index("--jobs") + 1] == "20"
    assert "--keep-going" in command
    assert "--default-resources" in command
    assert "disk_mb=2000" in command
    assert str(output_dir / ".galerna" / "done" / "bulk_0000.done") in command


def test_snakemake_slurm_command_defaults_to_unlimited_jobs(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1]},
        command="echo {{station}}",
        run={"backend": "snakemake", "mode": "cases", "executor": "slurm"},
    )
    commands = []

    def capture_run(command, check):
        commands.append(command)

    monkeypatch.setattr(subprocess, "run", capture_run)

    wrapper.run_cases()

    command = commands[0]
    assert "--jobs" in command
    assert command[command.index("--jobs") + 1] == "unlimited"


def test_snakemake_local_command_uses_cli_cores(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1]},
        command="echo {{station}}",
        run={
            "backend": "snakemake",
            "mode": "cases",
            "executor": "local",
            "snakemake": {"cli": {"cores": 2}},
        },
    )
    commands = []

    def capture_run(command, check):
        commands.append(command)

    monkeypatch.setattr(subprocess, "run", capture_run)

    wrapper.run_cases()

    command = commands[0]
    assert "--cores" in command
    assert command[command.index("--cores") + 1] == "2"


def test_snakemake_rejects_old_run_keys(tmp_path):
    with pytest.raises(ValueError, match="Unsupported run key"):
        Galerna(
            output_dir=str(tmp_path / "output"),
            variable_parameters={"station": [1]},
            command="echo {{station}}",
            run={
                "backend": "snakemake",
                "mode": "bulk",
                "executor": "local",
                "tasks_per_job": 2,
            },
        )


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
            "snakemake": {"cli": {"cores": 1}},
        },
    )

    wrapper.run_cases(cases=[1])

    assert not (output_dir / "0000" / "result_0000.txt").exists()
    assert (output_dir / "0001" / "result_0001.txt").read_text() == "2"
    assert not (output_dir / "0000" / ".galerna.done").exists()
    assert (output_dir / "0001" / ".galerna.done").is_file()

    statuses = wrapper.status_cases()
    assert [row["status"] for row in statuses] == ["NOT_BUILT", "DONE"]
    status_rows = read_status_rows(
        output_dir / ".galerna" / "status" / "status_0001.tsv"
    )
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
            "snakemake": {"cli": {"cores": 1}},
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


@pytest.mark.skipif(shutil.which("snakemake") is None, reason="snakemake not found")
def test_snakemake_bulk_local_executor_runs_complete_group(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2, 3, 4]},
        command=(
            "python -c 'from pathlib import Path; "
            'Path(\"result_{{case_id}}.txt\").write_text(\"{{station}}\")\''
        ),
        run={
            "backend": "snakemake",
            "mode": "bulk",
            "executor": "local",
            "cases_per_job": 2,
            "snakemake": {
                "rule": {"threads": 2},
                "cli": {"cores": 2},
            },
        },
    )

    wrapper.run_cases(cases=[0, 1])

    assert (output_dir / "0000" / "result_0000.txt").read_text() == "1"
    assert (output_dir / "0001" / "result_0001.txt").read_text() == "2"
    assert not (output_dir / "0002" / "result_0002.txt").exists()
    assert (output_dir / ".galerna" / "done" / "bulk_0000.done").is_file()
    assert not (output_dir / ".galerna" / "done" / "bulk_0001.done").exists()

    statuses = wrapper.status_cases()
    assert [row["status"] for row in statuses] == [
        "DONE",
        "DONE",
        "NOT_BUILT",
        "NOT_BUILT",
    ]


def test_snakemake_bulk_rejects_partial_case_group(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2, 3, 4]},
        command="echo {{station}}",
        run={
            "backend": "snakemake",
            "mode": "bulk",
            "executor": "local",
            "cases_per_job": 2,
        },
    )

    with pytest.raises(ValueError, match="complete case groups"):
        wrapper.run_cases(cases=[1])
