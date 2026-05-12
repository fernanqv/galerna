import csv
import itertools
import logging
import os
import re
import shlex
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, Template

from .utils import copy_files, get_simple_logger

DEFAULT_CASE_ID_FORMAT = '{{ "%04d" | format(case_num) }}'
DEBUG_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
EXECUTION_STATUSES = {"BUILT", "STARTED", "DONE", "FAILED", "SKIPPED"}


@dataclass
class CasesConfig:
    layout: str = "directories"
    id_format: str | Callable[[dict], str] = DEFAULT_CASE_ID_FORMAT


@dataclass
class WorkflowConfig:
    source: str = "generated"
    file: str | None = None
    profile: str | None = None
    config: str | None = None


@dataclass
class RunConfig:
    backend: str = "local"
    mode: str = "cases"
    executor: str = "local"
    cores: int = 1
    cpus_per_task: int = 1
    tasks_per_job: int = 1
    max_jobs: int | None = None
    partition: str | None = None
    runtime: int | None = None
    mem_mb: int | None = None
    workflow: WorkflowConfig = field(default_factory=WorkflowConfig)


@dataclass
class StatusConfig:
    mode: str = "auto"


class Galerna:
    """
    Base class for building parametric case directories and manifests.

    The clean Galerna workflow is:

    - build cases from variable and fixed parameters;
    - write a manifest under ``<output_dir>/.galerna/cases.tsv``;
    - let local or workflow backends consume that manifest.
    """

    def __init__(
        self,
        templates_dir: str | None = None,
        variable_parameters: dict | str | None = None,
        fixed_parameters: dict | None = None,
        output_dir: str = "output",
        templates_name: list[str] | str = "all",
        mode: str = "one_by_one",
        cases: dict | None = None,
        run: dict | None = None,
        status: dict | None = None,
        log_level: str = "INFO",
        log_file: str | None = None,
        log_console: bool | None = None,
        command: str | None = None,
    ) -> None:
        if log_console is None:
            log_console = log_file is None

        self._logger = get_simple_logger(
            name=self.__class__.__name__,
            level=log_level,
            log_file=log_file,
            console=log_console,
            console_format=(
                DEBUG_LOG_FORMAT if log_level.upper() == "DEBUG" else "%(message)s"
            ),
        )

        self.templates_dir = templates_dir
        self.variable_parameters = self._load_variable_parameters(variable_parameters)
        self.fixed_parameters = fixed_parameters or {}
        self.output_dir = str(output_dir)
        self.galerna_dir = str(Path(self.output_dir) / ".galerna")
        self.templates_name = templates_name
        self.mode = mode
        self.command = command
        self.cases_config = self._normalize_cases_config(cases)
        self.run_config = self._normalize_run_config(run)
        self.status_config = self._normalize_status_config(status)

        self._validate_config()
        self._env = self._create_template_env()
        self.cases_context: list[dict] = []
        self._generate_cases_context()

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def env(self) -> Environment | None:
        return self._env

    @property
    def cases_dirs(self) -> list[str]:
        return [ctx["case_dir"] for ctx in self.cases_context]

    @property
    def manifest_path(self) -> str:
        return str(Path(self.galerna_dir) / "cases.tsv")

    @property
    def snakefile_path(self) -> str:
        return str(Path(self.galerna_dir) / "Snakefile")

    @property
    def normalized_config(self) -> dict:
        return {
            "output_dir": self.output_dir,
            "mode": self.mode,
            "cases": {
                "layout": self.cases_config.layout,
                "id_format": self.cases_config.id_format,
            },
            "run": {
                "backend": self.run_config.backend,
                "mode": self.run_config.mode,
                "executor": self.run_config.executor,
                "cores": self.run_config.cores,
                "cpus_per_task": self.run_config.cpus_per_task,
                "tasks_per_job": self.run_config.tasks_per_job,
                "max_jobs": self.run_config.max_jobs,
                "partition": self.run_config.partition,
                "runtime": self.run_config.runtime,
                "mem_mb": self.run_config.mem_mb,
                "workflow": {
                    "source": self.run_config.workflow.source,
                    "file": self.run_config.workflow.file,
                    "profile": self.run_config.workflow.profile,
                    "config": self.run_config.workflow.config,
                },
            },
            "status": {"mode": self.status_config.mode},
        }

    def _load_variable_parameters(self, variable_parameters: dict | str | None) -> dict:
        if variable_parameters is None:
            return {}

        if isinstance(variable_parameters, str):
            import yaml

            if not os.path.isfile(variable_parameters):
                raise FileNotFoundError(
                    f"variable_parameters file not found: {variable_parameters}"
                )
            with open(variable_parameters) as f:
                return yaml.safe_load(f) or {}

        return variable_parameters

    def _normalize_cases_config(self, cases: dict | None) -> CasesConfig:
        cases = cases or {}
        return CasesConfig(
            layout=cases.get("layout", "directories"),
            id_format=cases.get("id_format", DEFAULT_CASE_ID_FORMAT),
        )

    def _normalize_run_config(self, run: dict | None) -> RunConfig:
        run = run or {}
        workflow = run.get("workflow", {}) or {}
        return RunConfig(
            backend=run.get("backend", "local"),
            mode=run.get("mode", "cases"),
            executor=run.get("executor", "local"),
            cores=run.get("cores", 1),
            cpus_per_task=run.get("cpus_per_task", 1),
            tasks_per_job=run.get("tasks_per_job", 1),
            max_jobs=run.get("max_jobs"),
            partition=run.get("partition"),
            runtime=run.get("runtime"),
            mem_mb=run.get("mem_mb"),
            workflow=WorkflowConfig(
                source=workflow.get("source", "generated"),
                file=workflow.get("file"),
                profile=workflow.get("profile"),
                config=workflow.get("config"),
            ),
        )

    def _normalize_status_config(self, status: dict | None) -> StatusConfig:
        status = status or {}
        return StatusConfig(mode=status.get("mode", "auto"))

    def _validate_config(self) -> None:
        if self.cases_config.layout not in {"directories", "shared"}:
            raise ValueError("cases.layout must be 'directories' or 'shared'.")

        if self.mode not in {"one_by_one", "all_combinations"}:
            raise ValueError("mode must be 'one_by_one' or 'all_combinations'.")

        if self.run_config.backend not in {"local", "snakemake", "nextflow"}:
            raise ValueError("run.backend must be 'local', 'snakemake', or 'nextflow'.")

        if self.run_config.backend == "nextflow":
            raise NotImplementedError(
                "run.backend: nextflow is reserved for future use."
            )

        if self.run_config.mode not in {"cases", "bulk"}:
            raise ValueError("run.mode must be 'cases' or 'bulk'.")

        if self.run_config.tasks_per_job < 1:
            raise ValueError("run.tasks_per_job must be greater than or equal to 1.")

        if self.run_config.cpus_per_task < 1:
            raise ValueError("run.cpus_per_task must be greater than or equal to 1.")

        if self.run_config.executor not in {"local", "slurm"}:
            raise ValueError("run.executor must be 'local' or 'slurm'.")

        if self.run_config.workflow.source not in {"generated", "user"}:
            raise ValueError("run.workflow.source must be 'generated' or 'user'.")

        if self.run_config.workflow.source == "generated" and not self.command:
            raise ValueError(
                "command is required when run.workflow.source is 'generated'."
            )

        if (
            self.run_config.workflow.source == "user"
            and not self.run_config.workflow.file
        ):
            raise ValueError(
                "run.workflow.file is required when workflow.source is 'user'."
            )

        if self.cases_config.layout == "shared" and self.templates_dir is not None:
            raise ValueError(
                "templates_dir is not supported with cases.layout: shared."
            )

    def _create_template_env(self) -> Environment | None:
        if self.templates_dir is None:
            return None

        if not os.path.isdir(self.templates_dir):
            raise FileNotFoundError(
                f"Template directory not found: {self.templates_dir}"
            )

        env = Environment(loader=FileSystemLoader(self.templates_dir))
        if self.templates_name == "all":
            self.templates_name = env.list_templates()
        return env

    def _generate_cases_context(self) -> None:
        variable_parameters = {
            key: self._expand_parameter_value(value)
            for key, value in self.variable_parameters.items()
        }

        if not variable_parameters:
            self.cases_context = [{}]
        elif self.mode == "all_combinations":
            keys = variable_parameters.keys()
            values = variable_parameters.values()
            self.cases_context = [
                dict(zip(keys, c, strict=False)) for c in itertools.product(*values)
            ]
        else:
            num_cases = len(next(iter(variable_parameters.values())))
            self._validate_one_by_one_lengths(variable_parameters, num_cases)
            self.cases_context = [
                {key: values[i] for key, values in variable_parameters.items()}
                for i in range(num_cases)
            ]

        for case_num, context in enumerate(self.cases_context):
            context["case_num"] = case_num
            context.update(self.fixed_parameters)
            context["case_id"] = self._render_case_id(context)
            context["case_dir"] = self._get_case_dir(context["case_id"])
            context["command_cmd"] = self._render_command(context)
            self._add_manifest_paths(context)

    def _expand_parameter_value(self, value: Any) -> Any:
        if isinstance(value, str) and value.strip().startswith("range("):
            return self._parse_range(value)
        return value

    def _parse_range(self, value: str) -> list[int]:
        match = re.fullmatch(
            r"range\(\s*(-?\d+)\s*,\s*(-?\d+)(?:\s*,\s*(-?\d+))?\s*\)",
            value.strip(),
        )
        if not match:
            raise ValueError(f"Invalid range expression: {value}")

        start = int(match.group(1))
        stop = int(match.group(2))
        step = int(match.group(3) or 1)
        return list(range(start, stop, step))

    def _validate_one_by_one_lengths(
        self, variable_parameters: dict[str, list], num_cases: int
    ) -> None:
        for key, values in variable_parameters.items():
            if not hasattr(values, "__len__"):
                raise TypeError(
                    f"variable_parameters.{key} must be a sequence in one_by_one mode."
                )
            if len(values) != num_cases:
                raise ValueError(
                    "All variable_parameters must have the same length in "
                    f"one_by_one mode. '{key}' has length {len(values)}, "
                    f"expected {num_cases}."
                )

    def _render_case_id(self, context: dict) -> str:
        id_format = self.cases_config.id_format
        if callable(id_format):
            return str(id_format(context))
        return Template(str(id_format)).render(context)

    def _get_case_dir(self, case_id: str) -> str:
        output_dir = Path(self.output_dir).resolve()
        if self.cases_config.layout == "shared":
            return str(output_dir)
        return str(output_dir / case_id)

    def _render_command(self, context: dict) -> str:
        if not self.command:
            return ""
        return Template(self.command).render(context)

    def _add_manifest_paths(self, context: dict) -> None:
        case_id = context["case_id"]
        case_dir = Path(context["case_dir"])
        galerna_dir = Path(self.galerna_dir).resolve()
        status_group = self._status_group_for_context(context)

        if self.cases_config.layout == "directories":
            context["stdout_log"] = str(case_dir / "galerna.out")
            context["stderr_log"] = str(case_dir / "galerna.err")
            context["status_file"] = str(case_dir / "galerna.status")
            context["status_group"] = status_group
            if (
                self.run_config.backend == "snakemake"
                and self.run_config.mode == "bulk"
            ):
                context["done_file"] = str(
                    galerna_dir / "done" / f"{status_group}.done"
                )
            else:
                context["done_file"] = str(case_dir / ".galerna.done")
            return

        context["stdout_log"] = str(galerna_dir / "logs" / f"{case_id}.out")
        context["stderr_log"] = str(galerna_dir / "logs" / f"{case_id}.err")
        context["status_group"] = status_group
        context["status_file"] = str(
            galerna_dir / "status" / f"status_{context['status_group']}.tsv"
        )
        done_group = self._done_group_for_shared_layout(context)
        context["done_file"] = str(
            galerna_dir / "done" / f"{done_group}.done"
        )

    def _status_group_for_context(self, context: dict) -> str:
        if self.run_config.mode != "bulk":
            if self.cases_config.layout == "shared":
                return "cases"
            return context["case_id"]
        group_num = context["case_num"] // self.run_config.tasks_per_job
        return f"bulk_{group_num:04d}"

    def _done_group_for_shared_layout(self, context: dict) -> str:
        if (
            self.run_config.backend == "snakemake"
            and self.run_config.mode == "cases"
        ):
            return context["case_id"]
        return context["status_group"]

    def build_case(self, case_context: dict) -> None:
        """Hook for subclasses to add custom case build logic."""

    def get_context(self) -> list[dict] | Any:
        try:
            import pandas as pd

            return pd.DataFrame(self.cases_context)
        except ImportError:
            return self.cases_context

    def build_cases(self, cases: list[int] | None = None) -> int:
        contexts_to_build = self._select_contexts(cases)
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.galerna_dir).mkdir(parents=True, exist_ok=True)

        if self.cases_config.layout == "shared":
            self._build_shared_layout(contexts_to_build)
        else:
            self._build_directory_layout(contexts_to_build)

        self.write_manifest()
        self.write_workflow_artifacts()
        return len(contexts_to_build)

    def _select_contexts(self, cases: list[int] | None = None) -> list[dict]:
        if cases is None:
            return self.cases_context
        return [self.cases_context[i] for i in cases]

    def _build_shared_layout(self, contexts: list[dict]) -> None:
        for context in contexts:
            self.build_case(context)
            self._mark_built(context)

    def _build_directory_layout(self, contexts: list[dict]) -> None:
        for context in contexts:
            case_dir = Path(context["case_dir"])
            self.logger.debug(
                "Building case %s in %s", context.get("case_num"), case_dir
            )
            case_dir.mkdir(parents=True, exist_ok=True)
            self.build_case(context)
            self._render_templates(context)
            self._mark_built(context)

    def _render_templates(self, context: dict) -> None:
        if self.env is None:
            return

        case_dir = Path(context["case_dir"])
        for template_name in self.templates_name:
            src_path = Path(self.templates_dir) / template_name
            dst_path = case_dir / template_name
            dst_path.parent.mkdir(parents=True, exist_ok=True)

            if src_path.is_symlink():
                self._copy_symlink(src_path, dst_path)
                continue

            try:
                template = self.env.get_template(template_name)
                dst_path.write_text(template.render(context))
            except Exception:
                copy_files(str(src_path), str(dst_path))

    def _copy_symlink(self, src_path: Path, dst_path: Path) -> None:
        link_target = src_path.resolve()
        if dst_path.exists() or dst_path.is_symlink():
            dst_path.unlink()
        dst_path.symlink_to(link_target)
        self.logger.debug("Created symlink %s -> %s", dst_path, link_target)

    def write_manifest(self) -> None:
        fieldnames = [
            "case_id",
            "case_num",
            "case_dir",
            "command",
            "stdout_log",
            "stderr_log",
            "status_file",
            "done_file",
            "status_group",
        ]
        manifest_path = Path(self.manifest_path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        with manifest_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            for context in self.cases_context:
                writer.writerow(
                    {
                        "case_id": context["case_id"],
                        "case_num": context["case_num"],
                        "case_dir": context["case_dir"],
                        "command": context["command_cmd"],
                        "stdout_log": context["stdout_log"],
                        "stderr_log": context["stderr_log"],
                        "status_file": context["status_file"],
                        "done_file": context["done_file"],
                        "status_group": context["status_group"],
                    }
                )

        self.logger.debug("Cases manifest saved to %s", manifest_path)

    def write_workflow_artifacts(self) -> None:
        if self.run_config.backend != "snakemake":
            return
        if self.run_config.workflow.source == "user":
            return
        if self.run_config.mode == "cases":
            self.write_snakemake_cases_workflow()
            return
        if self.run_config.mode == "bulk":
            self.write_snakemake_bulk_workflow()
            return
        raise NotImplementedError(
            f"Snakemake artifact generation does not support run.mode: "
            f"{self.run_config.mode}."
        )

    def write_snakemake_cases_workflow(self) -> None:
        snakefile_path = Path(self.snakefile_path)
        snakefile_path.parent.mkdir(parents=True, exist_ok=True)

        rule_blocks = []
        for context in self.cases_context:
            case_num = context["case_num"]
            case_id = context["case_id"]
            rule_blocks.append(
                "\n".join(
                    [
                        f"rule case_{case_num}:",
                        f"    output: {str(context['done_file'])!r}",
                        f"    log: stdout={str(context['stdout_log'])!r}, "
                        f"stderr={str(context['stderr_log'])!r}",
                        f"    threads: {self.run_config.cpus_per_task}",
                        *self._snakemake_rule_resource_lines(),
                        f"    params: case_id={case_id!r}",
                        "    run:",
                        "        run_case(CASES[params.case_id])",
                    ]
                )
            )

        done_files = [context["done_file"] for context in self.cases_context]
        snakefile = "\n\n".join(
            [
                self._snakemake_cases_prelude(),
                "rule all:\n"
                f"    input: {done_files!r}",
                *rule_blocks,
            ]
        )
        snakefile_path.write_text(snakefile + "\n")
        self.logger.debug("Snakefile saved to %s", snakefile_path)

    def write_snakemake_bulk_workflow(self) -> None:
        snakefile_path = Path(self.snakefile_path)
        snakefile_path.parent.mkdir(parents=True, exist_ok=True)

        rule_blocks = []
        for group_id, group_contexts in self._bulk_groups().items():
            case_ids = [context["case_id"] for context in group_contexts]
            done_file = group_contexts[0]["done_file"]
            rule_blocks.append(
                "\n".join(
                    [
                        f"rule {group_id}:",
                        f"    output: {str(done_file)!r}",
                        f"    threads: {self.run_config.cpus_per_task}",
                        *self._snakemake_rule_resource_lines(),
                        f"    params: case_ids={case_ids!r}",
                        "    run:",
                        "        run_bulk(params.case_ids, output[0], threads)",
                    ]
                )
            )

        done_files = [
            group_contexts[0]["done_file"]
            for group_contexts in self._bulk_groups().values()
        ]
        snakefile = "\n\n".join(
            [
                self._snakemake_cases_prelude(),
                self._snakemake_bulk_prelude(),
                "rule all:\n"
                f"    input: {done_files!r}",
                *rule_blocks,
            ]
        )
        snakefile_path.write_text(snakefile + "\n")
        self.logger.debug("Snakefile saved to %s", snakefile_path)

    def _snakemake_rule_resource_lines(self) -> list[str]:
        resources = []
        if self.run_config.mem_mb is not None:
            resources.append(f"mem_mb={self.run_config.mem_mb}")
        if self.run_config.runtime is not None:
            resources.append(f"runtime={self.run_config.runtime}")
        if self.run_config.partition is not None:
            resources.append(f"slurm_partition={self.run_config.partition!r}")
        if not resources:
            return []
        return [f"    resources: {', '.join(resources)}"]

    def _snakemake_cases_prelude(self) -> str:
        layout = self.cases_config.layout
        return f'''import csv
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

MANIFEST = Path({str(Path(self.manifest_path).resolve())!r})
LAYOUT = {layout!r}
CASES = {{}}
STATUS_LOCKS = {{}}
with MANIFEST.open(newline="") as f:
    reader = csv.DictReader(f, delimiter="\\t")
    for row in reader:
        CASES[row["case_id"]] = row


def status_lock(status_file):
    status_key = str(status_file)
    if status_key not in STATUS_LOCKS:
        STATUS_LOCKS[status_key] = threading.Lock()
    return STATUS_LOCKS[status_key]


def append_status(row, status, message):
    status_file = Path(row["status_file"])
    status_file.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).isoformat()
    if LAYOUT == "directories":
        fieldnames = ["timestamp", "status", "message"]
        status_row = {{
            "timestamp": timestamp,
            "status": status,
            "message": message,
        }}
    else:
        fieldnames = ["timestamp", "case_id", "status", "message"]
        status_row = {{
            "timestamp": timestamp,
            "case_id": row["case_id"],
            "status": status,
            "message": message,
        }}

    with status_lock(status_file):
        write_header = not status_file.exists()
        with status_file.open("a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\\t")
            if write_header:
                writer.writeheader()
            writer.writerow(status_row)


def run_case(row, touch_done=True):
    done_file = Path(row["done_file"])
    stdout_log = Path(row["stdout_log"])
    stderr_log = Path(row["stderr_log"])
    done_file.parent.mkdir(parents=True, exist_ok=True)
    stdout_log.parent.mkdir(parents=True, exist_ok=True)
    stderr_log.parent.mkdir(parents=True, exist_ok=True)
    if done_file.exists():
        done_file.unlink()

    append_status(row, "STARTED", "")
    with stdout_log.open("w") as stdout, stderr_log.open("w") as stderr:
        result = subprocess.run(
            row["command"],
            shell=True,
            cwd=row["case_dir"],
            stdout=stdout,
            stderr=stderr,
            text=True,
        )

    if result.returncode == 0:
        append_status(row, "DONE", "exit_code=0")
        if touch_done:
            done_file.touch()
        return

    append_status(row, "FAILED", f"exit_code={{result.returncode}}")
    raise subprocess.CalledProcessError(result.returncode, row["command"])
'''

    def _snakemake_bulk_prelude(self) -> str:
        return '''

def run_bulk(case_ids, done_file, threads):
    done_path = Path(done_file)
    done_path.parent.mkdir(parents=True, exist_ok=True)
    if done_path.exists():
        done_path.unlink()

    workers = max(1, int(threads))
    errors = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(run_case, CASES[case_id], False)
            for case_id in case_ids
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except subprocess.CalledProcessError as exc:
                errors.append(exc)

    if errors:
        raise errors[0]

    done_path.touch()
'''

    def run_cases(
        self,
        cases: list[int] | None = None,
        progress: Callable[[str, dict, int, int], None] | None = None,
    ) -> None:
        if self.run_config.backend == "snakemake":
            self.run_cases_snakemake(cases=cases)
            return

        if self.run_config.backend != "local":
            raise NotImplementedError(
                f"run.backend: {self.run_config.backend} is not implemented yet."
            )

        contexts_to_run = self._select_contexts(cases)
        self._ensure_cases_built(contexts_to_run)
        total = len(contexts_to_run)

        for position, context in enumerate(contexts_to_run, start=1):
            if progress:
                progress("start", context, position, total)
            try:
                self._run_case_local(context)
            except subprocess.CalledProcessError:
                if progress:
                    progress("failed", context, position, total)
                raise
            if progress:
                progress("done", context, position, total)

    def run_cases_snakemake(self, cases: list[int] | None = None) -> None:
        if self.run_config.mode not in {"cases", "bulk"}:
            raise NotImplementedError(
                f"run.backend: snakemake does not support run.mode: "
                f"{self.run_config.mode}."
            )

        contexts_to_run = self._select_contexts(cases)
        self._ensure_cases_built(contexts_to_run)
        self.write_workflow_artifacts()

        workflow_file = self._snakemake_workflow_file()
        targets = self._snakemake_targets(contexts_to_run, cases)
        command = self._snakemake_command(workflow_file, targets)
        self.logger.debug("Running Snakemake command: %s", shlex.join(command))
        subprocess.run(command, check=True)

    def _snakemake_command(self, workflow_file: str, targets: list[str]) -> list[str]:
        command = [
            "snakemake",
            "--snakefile",
            workflow_file,
            "--rerun-incomplete",
        ]

        if self.run_config.executor == "local":
            command.extend(["--cores", str(self.run_config.cores)])
            command.extend(targets)
        elif self.run_config.executor == "slurm":
            command.extend(["--executor", "slurm"])
            jobs = self.run_config.max_jobs or self.run_config.cores
            command.extend(["--jobs", str(jobs)])
            command.extend(targets)
            command.extend(self._snakemake_default_resource_args())
        else:
            raise NotImplementedError(
                f"run.backend: snakemake does not support run.executor: "
                f"{self.run_config.executor}."
            )

        return command

    def _snakemake_default_resource_args(self) -> list[str]:
        resources = []
        if self.run_config.mem_mb is not None:
            resources.append(f"mem_mb={self.run_config.mem_mb}")
        if self.run_config.runtime is not None:
            resources.append(f"runtime={self.run_config.runtime}")
        if self.run_config.partition is not None:
            resources.append(f"slurm_partition={self.run_config.partition}")
        if not resources:
            return []
        return ["--default-resources", *resources]

    def _snakemake_targets(
        self, contexts_to_run: list[dict], cases: list[int] | None
    ) -> list[str]:
        if self.run_config.mode == "cases":
            return [context["done_file"] for context in contexts_to_run]

        selected_groups = self._validate_bulk_selection(cases)
        return [
            group_contexts[0]["done_file"]
            for group_id, group_contexts in self._bulk_groups().items()
            if group_id in selected_groups
        ]

    def _validate_bulk_selection(self, cases: list[int] | None) -> set[str]:
        groups = self._bulk_groups()
        if cases is None:
            return set(groups)

        selected_cases = set(cases)
        selected_groups = set()
        partial_groups = []
        for group_id, group_contexts in groups.items():
            group_cases = {context["case_num"] for context in group_contexts}
            overlap = selected_cases & group_cases
            if not overlap:
                continue
            if overlap != group_cases:
                expected = ",".join(str(case) for case in sorted(group_cases))
                partial_groups.append(f"{group_id} ({expected})")
            selected_groups.add(group_id)

        if partial_groups:
            groups_str = "; ".join(partial_groups)
            raise ValueError(
                "Snakemake bulk runs require complete case groups. "
                f"Select full groups: {groups_str}."
            )
        return selected_groups

    def _bulk_groups(self) -> dict[str, list[dict]]:
        groups: dict[str, list[dict]] = {}
        for context in self.cases_context:
            groups.setdefault(context["status_group"], []).append(context)
        return groups

    def _snakemake_workflow_file(self) -> str:
        if self.run_config.workflow.source == "user":
            if self.run_config.workflow.file is None:
                raise ValueError("run.workflow.file is required for user workflows.")
            return self.run_config.workflow.file
        return self.snakefile_path

    def _ensure_cases_built(self, contexts: list[dict]) -> None:
        unbuilt_cases = [
            context["case_num"]
            for context in contexts
            if self._case_needs_build(context)
        ]
        if unbuilt_cases or not Path(self.manifest_path).exists():
            self.build_cases(cases=unbuilt_cases)

    def _case_needs_build(self, context: dict) -> bool:
        if self.cases_config.layout == "directories" and not Path(
            context["case_dir"]
        ).exists():
            return True
        return self._latest_status(context, execution=True) is None

    def _run_case_local(self, context: dict) -> None:
        command = context["command_cmd"]
        if not command:
            raise ValueError("command is required to run cases locally.")

        case_dir = Path(context["case_dir"])
        stdout_log = Path(context["stdout_log"])
        stderr_log = Path(context["stderr_log"])
        done_file = Path(context["done_file"])

        stdout_log.parent.mkdir(parents=True, exist_ok=True)
        stderr_log.parent.mkdir(parents=True, exist_ok=True)
        done_file.parent.mkdir(parents=True, exist_ok=True)
        self._remove_done_marker(context)

        self._append_status(context, "STARTED", "")
        self.logger.debug(
            "Running case %s in %s with command=%s",
            context["case_id"],
            case_dir,
            command,
        )

        with stdout_log.open("w") as stdout, stderr_log.open("w") as stderr:
            result = subprocess.run(
                command,
                shell=True,
                cwd=case_dir,
                stdout=stdout,
                stderr=stderr,
                text=True,
            )

        if result.returncode == 0:
            self._append_status(context, "DONE", "exit_code=0")
            self._mark_done(context)
            return

        self._append_status(context, "FAILED", f"exit_code={result.returncode}")
        raise subprocess.CalledProcessError(result.returncode, command)

    def _append_status(self, context: dict, status: str, message: str) -> None:
        status_file = Path(context["status_file"])
        status_file.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).isoformat()

        if self.cases_config.layout == "directories":
            fieldnames = ["timestamp", "status", "message"]
            row = {
                "timestamp": timestamp,
                "status": status,
                "message": message,
            }
        else:
            fieldnames = ["timestamp", "case_id", "status", "message"]
            row = {
                "timestamp": timestamp,
                "case_id": context["case_id"],
                "status": status,
                "message": message,
            }

        write_header = not status_file.exists()
        with status_file.open("a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
            if write_header:
                writer.writeheader()
            writer.writerow(row)

    def _mark_built(self, context: dict) -> None:
        if self._latest_status(context, execution=True) is not None:
            return
        self._append_status(context, "BUILT", "case built")

    def _remove_done_marker(self, context: dict) -> None:
        done_file = Path(context["done_file"])
        if done_file.exists():
            done_file.unlink()

    def _mark_done(self, context: dict) -> None:
        if self.cases_config.layout == "directories":
            Path(context["done_file"]).touch()
            return

        if self._all_group_cases_done(context["status_group"]):
            Path(context["done_file"]).touch()

    def _all_group_cases_done(self, status_group: str) -> bool:
        group_contexts = [
            context
            for context in self.cases_context
            if context["status_group"] == status_group
        ]
        return all(
            self._latest_status(context, execution=True) == "DONE"
            for context in group_contexts
        )

    def _latest_status_row(
        self, context: dict, execution: bool = False
    ) -> dict[str, str] | None:
        status_file = Path(context["status_file"])
        if not status_file.exists():
            return None

        latest_row = None
        with status_file.open(newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                if self.cases_config.layout == "shared":
                    if row.get("case_id") != context["case_id"]:
                        continue
                if execution and row.get("status") not in EXECUTION_STATUSES:
                    continue
                latest_row = row
        return latest_row

    def _latest_status(self, context: dict, execution: bool = False) -> str | None:
        latest_row = self._latest_status_row(context, execution=execution)
        if latest_row is None:
            return None
        return latest_row.get("status")

    def _status_contexts(self, cases: list[int] | None = None) -> list[dict]:
        if not Path(self.manifest_path).exists():
            return self._select_contexts(cases)

        with Path(self.manifest_path).open(newline="") as f:
            contexts = list(csv.DictReader(f, delimiter="\t"))

        for context in contexts:
            context["case_num"] = int(context["case_num"])

        if cases is None:
            return contexts

        selected_cases = set(cases)
        selected_contexts = [
            context for context in contexts if context["case_num"] in selected_cases
        ]
        found_cases = {context["case_num"] for context in selected_contexts}
        missing_cases = selected_cases - found_cases
        if missing_cases:
            missing = ", ".join(str(case) for case in sorted(missing_cases))
            raise IndexError(f"Case index not found in manifest: {missing}")
        return selected_contexts

    def status_cases(
        self, cases: list[int] | None = None, execution: bool = False
    ) -> list[dict[str, str]]:
        contexts_to_check = self._status_contexts(cases)
        statuses = []
        for context in contexts_to_check:
            latest_row = self._latest_status_row(context, execution=execution)
            if latest_row is None:
                statuses.append(
                    {
                        "case_id": context["case_id"],
                        "status": "NOT_BUILT",
                        "timestamp": "",
                        "message": "",
                        "done": "yes" if Path(context["done_file"]).exists() else "no",
                    }
                )
                continue

            statuses.append(
                {
                    "case_id": context["case_id"],
                    "status": latest_row.get("status", ""),
                    "timestamp": latest_row.get("timestamp", ""),
                    "message": latest_row.get("message", ""),
                    "done": "yes" if Path(context["done_file"]).exists() else "no",
                }
            )
        return statuses

    def postprocess_case(self, case_context: dict, **kwargs) -> None:
        raise NotImplementedError("The method postprocess_case must be implemented.")

    def postprocess_cases(
        self,
        cases: list[int] | None = None,
        clean_after: bool = False,
        overwrite: bool = False,
        **kwargs,
    ) -> list[Any]:
        contexts_to_process = self._select_contexts(cases)
        results = []
        for context in contexts_to_process:
            result = self.postprocess_case(
                context,
                overwrite=overwrite,
                clean_after=clean_after,
                **kwargs,
            )
            results.append(result)
        return results
