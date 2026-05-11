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


def test_cli_rejects_all_action():
    result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", "all"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "invalid choice" in result.stderr
