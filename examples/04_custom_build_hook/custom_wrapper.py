from pathlib import Path

from galerna import Galerna


class ExampleWrapper(Galerna):
    def build_case(self, case_context):
        case_dir = Path(case_context["case_dir"])
        derived_value = case_context["station"] * 10
        case_context["derived_value"] = derived_value
        (case_dir / "derived.txt").write_text(f"{derived_value}\n")
