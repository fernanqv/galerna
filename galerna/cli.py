import argparse
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import yaml

from galerna.base import Galerna

DEFAULT_CONFIG_FILE = "galerna.yaml"


def parse_cases(cases_str: str) -> list[int]:
    cases = set()
    for part in cases_str.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_str, end_str = part.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            cases.update(range(start, end + 1))
        else:
            cases.add(int(part))
    return sorted(list(cases))


def load_custom_wrapper(
    file_path: str, class_name: str = "CustomGalerna"
) -> type[Galerna]:
    """
    Dynamically loads a Galerna subclass from a .py file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Custom wrapper file not found: {file_path}")

    spec = importlib.util.spec_from_file_location("custom_wrapper", file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["custom_wrapper"] = module
    spec.loader.exec_module(module)

    # Try to find a subclass of Galerna if class_name is default
    if class_name == "CustomGalerna":
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, Galerna)
                and attr is not Galerna
            ):
                return attr

    try:
        return getattr(module, class_name)
    except AttributeError as exc:
        raise AttributeError(f"Module {file_path} has no class {class_name}") from exc


def resolve_config_path(
    config_path: str | None, parser: argparse.ArgumentParser
) -> str:
    if config_path:
        return config_path

    default_path = Path(DEFAULT_CONFIG_FILE)
    if default_path.exists():
        print(f"Using config: {DEFAULT_CONFIG_FILE}")
        return str(default_path)

    parser.error(
        f"No config file provided and ./{DEFAULT_CONFIG_FILE} was not found. "
        f"Use: galerna run --config path/to/{DEFAULT_CONFIG_FILE}"
    )


def _display_path(path: str | Path) -> str:
    path = Path(path)
    try:
        return str(path.resolve().relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(path)


def _print_run_artifacts(wrapper: Galerna) -> None:
    output_dir = _display_path(wrapper.output_dir)
    if wrapper.cases_config.layout == "shared":
        print(f"Logs: {output_dir}/.galerna/logs/<case_id>.out, <case_id>.err")
        print(f"Status: {output_dir}/.galerna/status/")
        return

    print(f"Logs: {output_dir}/<case_id>/galerna.out, galerna.err")
    print(f"Status: {output_dir}/<case_id>/galerna.status")


def _run_progress(verbose: bool):
    def report(event: str, context: dict, position: int, total: int) -> None:
        case_id = context["case_id"]
        if event == "start":
            if verbose:
                print(f"[{position}/{total}] {case_id}")
                print(f"  dir: {_display_path(context['case_dir'])}")
                print(f"  command: {context['command_cmd']}")
                return
            print(f"[{position}/{total}] {case_id} ... ", end="", flush=True)
        elif event == "done":
            print("done" if not verbose else "  done")
        elif event == "failed":
            print("failed" if not verbose else "  failed")
            print(f"  stdout: {_display_path(context['stdout_log'])}")
            print(f"  stderr: {_display_path(context['stderr_log'])}")
            print(f"  status: {_display_path(context['status_file'])}")

    return report


def main():
    parser = argparse.ArgumentParser(
        description="CLI for building and running model wrappers."
    )
    parser.add_argument(
        "action",
        choices=["build", "run", "status", "postprocess"],
        help="Action to perform.",
    )
    parser.add_argument(
        "-c",
        "--config",
        help=(
            "Path to the YAML configuration file. "
            f"Defaults to ./{DEFAULT_CONFIG_FILE}."
        ),
    )
    parser.add_argument(
        "--cases",
        type=str,
        help="Comma-separated list of case indices or ranges (e.g., '1,2,5-7').",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show per-case directories and rendered commands.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show internal Galerna debug logs with timestamps.",
    )

    args = parser.parse_args()

    cases_list = None
    if args.cases is not None:
        try:
            cases_list = parse_cases(args.cases)
        except Exception as e:
            parser.error(f"Invalid format for --cases '{args.cases}': {e}")

    config_path = resolve_config_path(args.config, parser)
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    # Extract wrapper configuration.
    wrapper_config = config.get("wrapper", {}) or {}
    wrapper_code_path = wrapper_config.get("code", config.get("wrapper_code"))
    wrapper_class_name = wrapper_config.get(
        "class", config.get("wrapper_class", "CustomGalerna")
    )

    if wrapper_code_path:
        print(f"Loading custom wrapper from {wrapper_code_path}...")
        WrapperClass = load_custom_wrapper(wrapper_code_path, wrapper_class_name)
    else:
        WrapperClass = Galerna

    # Instantiate the wrapper
    # Remove CLI-specific keys from config to pass as kwargs
    wrapper_params = config.copy()
    for key in ["wrapper", "wrapper_code", "wrapper_class"]:
        wrapper_params.pop(key, None)
    wrapper_params["log_console"] = args.debug
    if args.debug:
        wrapper_params["log_level"] = "DEBUG"

    wrapper = WrapperClass(**wrapper_params)

    if args.action == "build":
        contexts_to_build = wrapper._select_contexts(cases_list)
        print(f"Building {len(contexts_to_build)} case(s)...")
        built_count = wrapper.build_cases(cases=cases_list)
        print(f"Built {built_count} case(s)")
        print(f"Manifest: {_display_path(wrapper.manifest_path)}")

    if args.action == "run":
        contexts_to_run = wrapper._select_contexts(cases_list)
        print(f"Running {len(contexts_to_run)} case(s) locally, sequentially")
        try:
            wrapper.run_cases(
                cases=cases_list,
                progress=_run_progress(args.verbose or args.debug),
            )
        except subprocess.CalledProcessError as exc:
            print(f"\nFailed with exit code {exc.returncode}")
            _print_run_artifacts(wrapper)
            sys.exit(exc.returncode or 1)

        print(f"\nCompleted {len(contexts_to_run)}/{len(contexts_to_run)} case(s)")
        _print_run_artifacts(wrapper)

    if args.action == "postprocess":
        print("Postprocessing cases...")
        wrapper.postprocess_cases(cases=cases_list)

    if args.action == "status":
        if hasattr(wrapper, "status_cases"):
            print("Checking status of cases...")
            status_result = wrapper.status_cases(cases=cases_list)
            print(status_result)
        else:
            print("Status action not supported by this wrapper class.")


if __name__ == "__main__":
    main()
