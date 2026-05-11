import subprocess
import sys


def test_cli_uses_default_galerna_yaml(tmp_path):
    config_path = tmp_path / "galerna.yaml"
    config_path.write_text(
        "\n".join(
            [
                f"output_dir: {tmp_path / 'output'}",
                "variable_parameters:",
                "  station: [1]",
                'command: "echo {{station}}"',
            ]
        )
    )

    result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "build"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "Using config: galerna.yaml" in result.stdout
    assert "Building 1 case(s)..." in result.stdout
    assert "Built 1 case(s)" in result.stdout
    assert "Manifest: output/.galerna/cases.tsv" in result.stdout
    assert "INFO" not in result.stderr
    assert (tmp_path / "output" / ".galerna" / "cases.tsv").is_file()


def test_cli_config_short_option(tmp_path):
    config_path = tmp_path / "custom.yaml"
    output_dir = tmp_path / "custom_output"
    config_path.write_text(
        "\n".join(
            [
                f"output_dir: {output_dir}",
                "variable_parameters:",
                "  station: [1]",
                'command: "echo {{station}}"',
            ]
        )
    )

    subprocess.run(
        [sys.executable, "-m", "galerna.cli", "build", "-c", str(config_path)],
        cwd=tmp_path,
        check=True,
    )

    assert (output_dir / ".galerna" / "cases.tsv").is_file()


def test_cli_run_shows_clean_progress_and_summary(tmp_path):
    config_path = tmp_path / "galerna.yaml"
    config_path.write_text(
        "\n".join(
            [
                "output_dir: output",
                "variable_parameters:",
                "  station: [1, 2]",
                'command: "echo {{station}}"',
            ]
        )
    )

    result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "run"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "Running 2 case(s) locally, sequentially" in result.stdout
    assert "[1/2] 0000 ... done" in result.stdout
    assert "[2/2] 0001 ... done" in result.stdout
    assert "Completed 2/2 case(s)" in result.stdout
    assert "Logs: output/<case_id>/galerna.out, galerna.err" in result.stdout
    assert "Status: output/<case_id>/galerna.status" in result.stdout
    assert "command=echo" not in result.stdout
    assert "INFO" not in result.stderr


def test_cli_run_verbose_shows_case_directory_and_command(tmp_path):
    config_path = tmp_path / "galerna.yaml"
    config_path.write_text(
        "\n".join(
            [
                "output_dir: output",
                "variable_parameters:",
                "  station: [1]",
                'command: "echo {{station}}"',
            ]
        )
    )

    result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "run", "--verbose"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "[1/1] 0000" in result.stdout
    assert "  dir: output/0000" in result.stdout
    assert "  command: echo 1" in result.stdout
    assert "  done" in result.stdout


def test_cli_loads_wrapper_from_default_config(tmp_path):
    templates_dir = tmp_path / "templates"
    templates_dir.mkdir()
    (templates_dir / "run_case.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                'Path("result.txt").write_text("derived={{ derived }}\\n")',
            ]
        )
    )

    (tmp_path / "custom_wrapper.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "from galerna import Galerna",
                "",
                "class ExampleWrapper(Galerna):",
                "    def build_case(self, case_context):",
                '        case_dir = Path(case_context["case_dir"])',
                "        assert case_dir.is_dir()",
                '        case_context["derived"] = case_context["station"] * 10',
                '        (case_dir / "derived.txt").write_text(',
                '            str(case_context["derived"])',
                "        )",
            ]
        )
    )

    (tmp_path / "galerna.yaml").write_text(
        "\n".join(
            [
                "wrapper:",
                "  code: custom_wrapper.py",
                "  class: ExampleWrapper",
                "templates_dir: templates",
                "output_dir: output",
                "variable_parameters:",
                "  station: [3]",
                'command: "python run_case.py"',
            ]
        )
    )

    result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "run"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )

    case_dir = tmp_path / "output" / "0000"
    assert "Loading custom wrapper from custom_wrapper.py..." in result.stdout
    assert (case_dir / "derived.txt").read_text() == "30"
    assert (case_dir / "result.txt").read_text() == "derived=30\n"


def test_cli_debug_enables_internal_debug_logging(tmp_path):
    config_path = tmp_path / "galerna.yaml"
    config_path.write_text(
        "\n".join(
            [
                "output_dir: output",
                "variable_parameters:",
                "  station: [1]",
                'command: "echo {{station}}"',
            ]
        )
    )

    result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "build", "--debug"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "Built 1 case(s)" in result.stdout
    assert "Galerna - DEBUG - Building case 0" in result.stderr
    assert "Galerna - DEBUG - Cases manifest saved to output/.galerna/cases.tsv" in (
        result.stderr
    )


def test_cli_status_shows_latest_human_and_execution_status(tmp_path):
    config_path = tmp_path / "galerna.yaml"
    config_path.write_text(
        "\n".join(
            [
                "output_dir: output",
                "variable_parameters:",
                "  station: [1]",
                'command: "echo {{station}}"',
            ]
        )
    )

    subprocess.run(
        [sys.executable, "-m", "galerna.cli", "run"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    status_file = tmp_path / "output" / "0000" / "galerna.status"
    with status_file.open("a") as f:
        f.write("2026-05-11T10:00:00+00:00\tQC_OK\tchecked\n")

    human_result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "status"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    execution_result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "status", "--execution"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "case_id" in human_result.stdout
    assert "QC_OK" in human_result.stdout
    assert "checked" in human_result.stdout
    assert "DONE" in execution_result.stdout
    assert "QC_OK" not in execution_result.stdout


def test_cli_run_reports_bulk_partial_group_error_without_traceback(tmp_path):
    config_path = tmp_path / "galerna.yaml"
    config_path.write_text(
        "\n".join(
            [
                "output_dir: output",
                "variable_parameters:",
                "  station: [1, 2]",
                'command: "echo {{station}}"',
                "run:",
                "  backend: snakemake",
                "  mode: bulk",
                "  executor: local",
                "  tasks_per_job: 2",
            ]
        )
    )

    result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "run", "--cases", "1"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "Error: Snakemake bulk runs require complete case groups" in result.stdout
    assert "Traceback" not in result.stderr


def test_cli_rejects_all_action():
    result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "all"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "invalid choice" in result.stderr
