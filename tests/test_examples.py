import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_ROOT = REPO_ROOT / "examples"


def non_slurm_examples():
    examples = []
    for config_path in sorted(EXAMPLES_ROOT.glob("**/galerna.yaml")):
        config = yaml.safe_load(config_path.read_text()) or {}
        run_config = config.get("run", {}) or {}
        if run_config.get("executor") == "slurm":
            continue

        example_dir = config_path.parent
        marks = []
        if run_config.get("backend") == "snakemake":
            marks.append(
                pytest.mark.skipif(
                    shutil.which("snakemake") is None,
                    reason="snakemake not found",
                )
            )

        examples.append(
            pytest.param(
                example_dir,
                id=str(example_dir.relative_to(EXAMPLES_ROOT)),
                marks=marks,
            )
        )

    return examples


def run_galerna(action, cwd):
    result = subprocess.run(
        [sys.executable, "-m", "galerna.cli", action],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"galerna {action} failed in {cwd}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    return result


@pytest.mark.parametrize("example_dir", non_slurm_examples())
def test_non_slurm_examples_build_run_and_status(example_dir, tmp_path):
    workdir = tmp_path / "example"
    shutil.copytree(
        example_dir,
        workdir,
        ignore=shutil.ignore_patterns("runs", ".snakemake", "__pycache__"),
    )

    build = run_galerna("build", workdir)
    run = run_galerna("run", workdir)
    status = run_galerna("status", workdir)

    assert "Built " in build.stdout
    assert "Completed " in run.stdout
    assert "case_id" in status.stdout
    assert "DONE" in status.stdout
