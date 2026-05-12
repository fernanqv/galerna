from pathlib import Path

from galerna import Galerna


class CustomWrapper(Galerna):
    def build_case(self, case_context: dict) -> None:
        case_dir = Path(case_context["case_dir"])
        derived = case_context["station"] * 10
        case_context["derived"] = derived
        (case_dir / "derived.txt").write_text(f"{derived}\n")
