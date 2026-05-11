import csv
from pathlib import Path

from galerna import Galerna


def append_status(status_file: Path, status: str, message: str = "") -> None:
    with status_file.open("a", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["timestamp", "status", "message"], delimiter="\t"
        )
        writer.writerow(
            {
                "timestamp": "2026-05-11T10:00:00+00:00",
                "status": status,
                "message": message,
            }
        )


def append_shared_status(
    status_file: Path, case_id: str, status: str, message: str = ""
) -> None:
    with status_file.open("a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["timestamp", "case_id", "status", "message"],
            delimiter="\t",
        )
        writer.writerow(
            {
                "timestamp": "2026-05-11T10:00:00+00:00",
                "case_id": case_id,
                "status": status,
                "message": message,
            }
        )


def test_status_cases_reports_latest_human_status_and_pending_cases(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2]},
        command="echo {{station}}",
    )

    wrapper.run_cases(cases=[0])
    append_status(output_dir / "0000" / "galerna.status", "QC_OK", "checked")

    statuses = wrapper.status_cases()

    assert statuses == [
        {
            "case_id": "0000",
            "status": "QC_OK",
            "timestamp": "2026-05-11T10:00:00+00:00",
            "message": "checked",
            "done": "yes",
        },
        {
            "case_id": "0001",
            "status": "PENDING",
            "timestamp": "",
            "message": "",
            "done": "no",
        },
    ]


def test_status_cases_execution_view_ignores_custom_statuses(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1]},
        command="echo {{station}}",
    )

    wrapper.run_cases()
    append_status(output_dir / "0000" / "galerna.status", "TRANSFERRED")

    statuses = wrapper.status_cases(execution=True)

    assert statuses[0]["case_id"] == "0000"
    assert statuses[0]["status"] == "DONE"
    assert statuses[0]["message"] == "exit_code=0"
    assert statuses[0]["done"] == "yes"


def test_status_cases_can_filter_manifest_cases(tmp_path):
    output_dir = tmp_path / "output"
    wrapper = Galerna(
        output_dir=str(output_dir),
        variable_parameters={"station": [1, 2, 3]},
        command="echo {{station}}",
    )

    wrapper.run_cases(cases=[1])

    statuses = wrapper.status_cases(cases=[0, 1])

    assert [row["case_id"] for row in statuses] == ["0000", "0001"]
    assert [row["status"] for row in statuses] == ["PENDING", "DONE"]


def test_shared_done_uses_execution_status_after_custom_status(tmp_path):
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

    wrapper.run_cases(cases=[0])
    status_file = output_dir / ".galerna" / "status" / "status_cases.tsv"
    done_file = output_dir / ".galerna" / "done" / "cases.done"
    append_shared_status(status_file, "0000", "QC_OK")

    assert not done_file.exists()

    wrapper.run_cases(cases=[1])

    assert done_file.is_file()
    statuses = wrapper.status_cases()
    assert [row["status"] for row in statuses] == ["QC_OK", "DONE"]
    execution_statuses = wrapper.status_cases(execution=True)
    assert [row["status"] for row in execution_statuses] == ["DONE", "DONE"]
