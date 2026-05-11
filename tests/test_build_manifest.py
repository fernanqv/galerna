import csv

import pytest

from galerna import Galerna


def read_manifest(output_dir):
    manifest_path = output_dir / ".galerna" / "cases.tsv"
    with manifest_path.open(newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def test_minimal_build_writes_full_manifest(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2, 3]},
        command="echo {{station}}",
    )

    wrapper.build_cases()

    rows = read_manifest(output_dir)
    assert [row["case_id"] for row in rows] == ["0000", "0001", "0002"]
    assert [row["command"] for row in rows] == ["echo 1", "echo 2", "echo 3"]
    assert (output_dir / "0000").is_dir()
    assert (output_dir / "0001").is_dir()
    assert (output_dir / "0002").is_dir()


def test_build_cases_subset_builds_subset_but_manifest_has_all_cases(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2, 3, 4]},
        command="echo {{station}}",
    )

    wrapper.build_cases(cases=[1, 3])

    rows = read_manifest(output_dir)
    assert [row["case_id"] for row in rows] == ["0000", "0001", "0002", "0003"]
    assert not (output_dir / "0000").exists()
    assert (output_dir / "0001").is_dir()
    assert not (output_dir / "0002").exists()
    assert (output_dir / "0003").is_dir()


def test_shared_layout_does_not_create_case_directories(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        cases={"layout": "shared"},
        variable_parameters={"station": [1, 2]},
        command="echo {{station}} > result_{{case_id}}.txt",
    )

    wrapper.build_cases()

    rows = read_manifest(output_dir)
    assert len(rows) == 2
    assert {row["case_dir"] for row in rows} == {str(output_dir.resolve())}
    assert rows[0]["status_file"].endswith(".galerna/status/status_cases.tsv")
    assert rows[0]["done_file"].endswith(".galerna/done/cases.done")
    assert not (output_dir / "0000").exists()
    assert not (output_dir / "0001").exists()


def test_shared_layout_rejects_templates_dir(tmp_path):
    templates_dir = tmp_path / "templates"
    templates_dir.mkdir()

    with pytest.raises(ValueError, match="templates_dir is not supported"):
        Galerna(
            templates_dir=str(templates_dir),
            output_dir=str(tmp_path / "output"),
            cases={"layout": "shared"},
            variable_parameters={"station": [1]},
            command="echo {{station}}",
        )


def test_all_combinations_builds_cartesian_product(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        mode="all_combinations",
        variable_parameters={
            "station": [1, 2],
            "compiler": ["gcc", "intel"],
        },
        command="echo {{station}} {{compiler}}",
    )

    wrapper.build_cases()

    rows = read_manifest(output_dir)
    assert [row["command"] for row in rows] == [
        "echo 1 gcc",
        "echo 1 intel",
        "echo 2 gcc",
        "echo 2 intel",
    ]
