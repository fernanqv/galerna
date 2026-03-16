import os
import shutil
import shutil
from galerna import Galerna

def test_combined_logging():
    output_dir = "test_log_output"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # Launcher that output to both stdout and stderr
    # In bash: echo "stdout" && echo "stderr" >&2
    launcher = 'echo "stdout_msg" && echo "stderr_msg" >&2'

    variable_parameters = {"p": [1]}
    fixed_parameters = {}

    wrapper = Galerna(
        templates_dir=None,
        variable_parameters=variable_parameters,
        fixed_parameters=fixed_parameters,
        output_dir=output_dir,
        cases_name_format="case_{p}"
    )

    log_filename = "combined.log"
    wrapper.run_cases(launcher, stdout_log=log_filename, stderr_log=log_filename)

    case_dir = os.path.join(output_dir, "case_1")
    log_path = os.path.join(case_dir, log_filename)

    assert os.path.exists(log_path)
    with open(log_path, "r") as f:
        content = f.read()
        print(f"Log content:\n{content}")
        assert "stdout_msg" in content
        assert "stderr_msg" in content

def test_separate_logging():
    output_dir = "test_log_output_sep"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    launcher = 'echo "out" && echo "err" >&2'

    wrapper = Galerna(
        templates_dir=None,
        variable_parameters={"p": [1]},
        fixed_parameters={},
        output_dir=output_dir,
        cases_name_format="case_{p}"
    )

    wrapper.run_cases(launcher, stdout_log="out.log", stderr_log="err.log")

    case_dir = os.path.join(output_dir, "case_1")
    assert os.path.exists(os.path.join(case_dir, "out.log"))
    assert os.path.exists(os.path.join(case_dir, "err.log"))
    
    with open(os.path.join(case_dir, "out.log"), "r") as f:
        assert "out" in f.read()
    with open(os.path.join(case_dir, "err.log"), "r") as f:
        assert "err" in f.read()

if __name__ == "__main__":
    test_combined_logging()
    test_separate_logging()
