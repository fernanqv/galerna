"""Focused tests for `examples/directories/01_local_with_templates/galerna.yaml`.

These validate that `mode: all_combinations` produces the Cartesian
product of the provided `variable_parameters` values.
"""

import yaml
from pathlib import Path

from galerna import Galerna


def _params_for_example(path: Path, tmp_path: Path) -> dict:
    config = yaml.safe_load(path.read_text()) or {}
    params = config.copy()
    for key in ("wrapper", "wrapper_code", "wrapper_class"):
        params.pop(key, None)
    # Use an isolated output dir and avoid requiring templates on disk
    params["output_dir"] = str(tmp_path / "runs")
    params["templates_dir"] = None
    return params


def test_all_combinations_station_compiler(tmp_path):
    """`all_combinations` should produce the Cartesian product of lists.

    The example defines:
      station: [1, 2]
      compiler: ["gcc", "intel"]

    Expect 4 contexts with all (station, compiler) pairs.
    """
    example_path = Path("examples/directories/01_local_with_templates/galerna.yaml")
    params = _params_for_example(example_path, tmp_path)

    wrapper = Galerna(**params)

    assert wrapper.mode == "all_combinations"
    assert len(wrapper.cases_context) == 4

    pairs = {(c["station"], c["compiler"]) for c in wrapper.cases_context}
    expected = {(1, "gcc"), (1, "intel"), (2, "gcc"), (2, "intel")}
    assert pairs == expected

    # Command is defined as a static string in the example
    assert all(c["command_cmd"] == "python run_case.py" for c in wrapper.cases_context)


def test_one_by_one_station_compiler(tmp_path):
    """`one_by_one` should pair values by position, not Cartesian product.

    Using the same example inputs:
        station: [1, 2]
        compiler: ["gcc", "intel"]

    Expect 2 contexts: (1,gcc) and (2,intel).
    """
    example_path = Path("examples/directories/01_local_with_templates/galerna.yaml")
    params = _params_for_example(example_path, tmp_path)
    # override mode to one_by_one for this test
    params["mode"] = "one_by_one"

    wrapper = Galerna(**params)

    assert wrapper.mode == "one_by_one"
    assert len(wrapper.cases_context) == 2

    pairs = [(c["station"], c["compiler"]) for c in wrapper.cases_context]
    assert pairs == [(1, "gcc"), (2, "intel")]
    assert all(c["command_cmd"] == "python run_case.py" for c in wrapper.cases_context)