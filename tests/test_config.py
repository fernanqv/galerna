"""Tests for file-backed `variable_parameters` (CSV/TSV loader).

These tests exercise the short-string `file:PATH` syntax and the dict
form `{'file', 'format', 'column'}` that select a column from a headered TSV.
"""
import shutil
import yaml
from pathlib import Path

from galerna import Galerna


def _params_for_example(path: Path, tmp_path: Path) -> dict:
    config = yaml.safe_load(path.read_text()) or {}
    params = config.copy()
    for key in ("wrapper", "wrapper_code", "wrapper_class"):
        params.pop(key, None)
    params["output_dir"] = str(tmp_path / "runs")
    params["templates_dir"] = None
    return params


def test_variable_parameters_from_csv_file_string_syntax(tmp_path):
    csv_path = tmp_path / "stations.csv"
    csv_path.write_text("1\n2\n3\n")

    params = {
        "templates_dir": None,
        "output_dir": str(tmp_path / "runs"),
        "variable_parameters": {"station": f"file:{csv_path}"},
        "mode": "one_by_one",
        "command": "echo {{station}}",
    }

    g = Galerna(**params)
    assert len(g.cases_context) == 3
    assert [c["station"] for c in g.cases_context] == ["1", "2", "3"]


def test_variable_parameters_from_tsv_file_dict_syntax(tmp_path):
    tsv_path = tmp_path / "stations.tsv"
    tsv_path.write_text("id\tname\n10\tA\n20\tB\n")

    params = {
        "templates_dir": None,
        "output_dir": str(tmp_path / "runs"),
        "variable_parameters": {
            "station": {"file": str(tsv_path), "format": "tsv", "column": "id"}
        },
        "mode": "one_by_one",
        "command": "echo {{station}}",
    }

    g = Galerna(**params)
    assert len(g.cases_context) == 2
    assert [c["station"] for c in g.cases_context] == ["10", "20"]


def test_example_dict_file_syntax(tmp_path):
    src = Path("examples/directories/06_file_parameters")
    dst = tmp_path / "example"
    shutil.copytree(src, dst)

    example_path = dst / "galerna_tsv.yaml"
    params = _params_for_example(example_path, tmp_path)

    # Ensure file path is absolute so Galerna can find it regardless of cwd
    params["variable_parameters"]["station"]["file"] = str(dst / "stations.tsv")

    g = Galerna(**params)
    assert [c["station"] for c in g.cases_context] == ["10", "20"]


def test_example_short_string_file_syntax(tmp_path):
    src = Path("examples/directories/06_file_parameters")
    dst = tmp_path / "example"
    shutil.copytree(src, dst)

    example_path = dst / "galerna.yaml"
    params = _params_for_example(example_path, tmp_path)

    # Ensure the short-string file path is absolute so Galerna can find it
    params["variable_parameters"]["station"] = f"file:{dst / 'stations.csv'}"

    g = Galerna(**params)
    assert [c["station"] for c in g.cases_context] == ["101", "102", "103"]


def test_variable_parameters_from_yaml_file(tmp_path):
    # Create a YAML file that defines variable_parameters at top level
    vars_path = tmp_path / "vars.yaml"
    vars_path.write_text("station:\n  - a\n  - b\n")

    params = {
        "templates_dir": None,
        "output_dir": str(tmp_path / "runs"),
        "variable_parameters": str(vars_path),
        "mode": "one_by_one",
        "command": "echo {{station}}",
    }

    g = Galerna(**params)
    assert [c["station"] for c in g.cases_context] == ["a", "b"]


def test_custom_separator(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id;name\n1;A\n2;B\n")

    params = {
        "templates_dir": None,
        "output_dir": str(tmp_path / "runs"),
        "variable_parameters": {
            "station": {"file": str(path), "format": "csv", "sep": ";", "column": "id"}
        },
        "mode": "one_by_one",
        "command": "echo {{station}}",
    }

    g = Galerna(**params)
    assert [c["station"] for c in g.cases_context] == ["1", "2"]


def test_txt_and_tsv_format_detection(tmp_path):
    txt = tmp_path / "list.txt"
    txt.write_text("x\ny\n")
    params_txt = {
        "templates_dir": None,
        "output_dir": str(tmp_path / "runs"),
        "variable_parameters": {"val": f"file:{txt}"},
        "mode": "one_by_one",
        "command": "echo {{val}}",
    }
    gtxt = Galerna(**params_txt)
    assert [c["val"] for c in gtxt.cases_context] == ["x", "y"]

    tsv = tmp_path / "vals.tsv"
    tsv.write_text("a\tb\n1\t2\n")
    params_tsv = {
        "templates_dir": None,
        "output_dir": str(tmp_path / "runs"),
        "variable_parameters": {"v": f"file:{tsv}"},
        "mode": "one_by_one",
        "command": "echo {{v}}",
    }
    gtsv = Galerna(**params_tsv)
    assert [c["v"] for c in gtsv.cases_context] == ["a", "1"]


def test_header_returned_when_no_column(tmp_path):
    p = tmp_path / "hdr.tsv"
    p.write_text("id\tname\n10\tA\n")
    params = {
        "templates_dir": None,
        "output_dir": str(tmp_path / "runs"),
        "variable_parameters": {"station": {"file": str(p), "format": "tsv"}},
        "mode": "one_by_one",
        "command": "echo {{station}}",
    }
    g = Galerna(**params)
    # First returned value is the header first column
    assert [c["station"] for c in g.cases_context] == ["id", "10"]


def test_expanduser_path_handling(tmp_path):
    home = Path.home()
    fname = ".galerna_test_stations.csv"
    target = home / fname
    try:
        target.write_text("h1\nh2\n")
        params = {
            "templates_dir": None,
            "output_dir": str(tmp_path / "runs"),
            "variable_parameters": {"station": {"file": f"~/{fname}"}},
            "mode": "one_by_one",
            "command": "echo {{station}}",
        }
        g = Galerna(**params)
        assert [c["station"] for c in g.cases_context] == ["h1", "h2"]
    finally:
        try:
            target.unlink()
        except Exception:
            pass


def test_missing_file_and_unsupported_format_errors(tmp_path):
    missing = tmp_path / "nope.csv"
    params = {
        "templates_dir": None,
        "output_dir": str(tmp_path / "runs"),
        "variable_parameters": {"x": {"file": str(missing)}} ,
        "mode": "one_by_one",
        "command": "echo {{x}}",
    }
    try:
        Galerna(**params)
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Expected FileNotFoundError for missing file")

    pf = tmp_path / "bad.parquet"
    pf.write_text("nope")
    params2 = {"templates_dir": None, "output_dir": str(tmp_path / "runs"), "variable_parameters": {"x": {"file": str(pf)}} , "mode": "one_by_one", "command": "echo {{x}}"}
    try:
        Galerna(**params2)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for unsupported file format")


def test_missing_column_raises_keyerror(tmp_path):
    p = tmp_path / "c.tsv"
    p.write_text("id\tname\n1\ta\n")
    params = {"templates_dir": None, "output_dir": str(tmp_path / "runs"), "variable_parameters": {"s": {"file": str(p), "format": "tsv", "column": "missing"}}, "mode": "one_by_one", "command": "echo {{s}}"}
    try:
        Galerna(**params)
    except KeyError:
        pass
    else:
        raise AssertionError("Expected KeyError for missing column")
