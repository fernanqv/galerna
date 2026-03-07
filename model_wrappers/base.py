import copy
import itertools
import logging
import os
import os.path as op
import subprocess
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Any, Callable, Dict, List, Optional, Union

from jinja2 import Environment, FileSystemLoader

from .utils import copy_files, get_simple_logger, write_array_in_file


class ModelWrapper:
    """
    Base class for numerical models wrappers.
    Autonomous implementation without external core dependencies.

    Attributes
    ----------
    templates_dir : str
        The directory where the templates are searched.
    variable_parameters : dict
        The parameters to be used for all cases.
    fixed_parameters : dict
        The fixed parameters for the cases.
    output_dir : str
        The directory where the output cases are saved.
    num_workers : int
        Number of parallel workers to use.
    """

    available_launchers = {}

    def __init__(
        self,
        templates_dir: Optional[str],
        variable_parameters: dict,
        fixed_parameters: dict,
        output_dir: str,
        templates_name: Union[List[str], str] = "all",
        num_workers: int = 1,
    ) -> None:
        self._logger: Optional[logging.Logger] = None
        self.num_workers = num_workers
        
        self.templates_dir = templates_dir
        self.variable_parameters = variable_parameters
        self.fixed_parameters = fixed_parameters
        self.output_dir = output_dir

        if self.templates_dir is not None:
            self._env = Environment(loader=FileSystemLoader(self.templates_dir))
            if templates_name == "all":
                self.templates_name = self.env.list_templates()
            else:
                self.templates_name = templates_name
        else:
            self._env = None
            self.templates_name = []

        self.cases_context: Optional[List[dict]] = None
        self.cases_dirs: Optional[List[str]] = None
        self.thread: Optional[threading.Thread] = None
        self.status_queue: Optional[Queue] = None

    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = get_simple_logger(name=self.__class__.__name__)
        return self._logger

    @logger.setter
    def logger(self, value: logging.Logger) -> None:
        self._logger = value

    @property
    def env(self) -> Environment:
        return self._env

    def parallel_execute(
        self,
        func: Callable,
        items: List[Any],
        num_workers: int,
        cpu_intensive: bool = False,
        **kwargs,
    ) -> Dict[int, Any]:
        results = {}
        executor_class = ProcessPoolExecutor if cpu_intensive else ThreadPoolExecutor
        with executor_class(max_workers=num_workers) as executor:
            future_to_item = {
                executor.submit(func, *item, **kwargs)
                if isinstance(item, tuple)
                else executor.submit(func, item, **kwargs): i
                for i, item in enumerate(items)
            }
            for future in as_completed(future_to_item):
                i = future_to_item[future]
                try:
                    results[i] = future.result()
                except Exception as exc:
                    self.logger.error(f"Job {i} failed: {exc}")
        return results

    def load_cases(
        self,
        mode: str = "one_by_one",
        cases_name_format: Callable = lambda ctx: f"{ctx.get('case_num'):04}",
    ) -> None:
        if mode == "all_combinations":
            keys = self.variable_parameters.keys()
            values = self.variable_parameters.values()
            combinations = itertools.product(*values)
            self.cases_context = [dict(zip(keys, c)) for c in combinations]
        elif mode == "one_by_one":
            num_cases = len(next(iter(self.variable_parameters.values())))
            self.cases_context = []
            for i in range(num_cases):
                case = {p: v[i] for p, v in self.variable_parameters.items()}
                self.cases_context.append(case)
        else:
            raise ValueError(f"Invalid mode: {mode}")

        self.cases_dirs = []
        for i, context in enumerate(self.cases_context):
            context["case_num"] = i
            name = cases_name_format(context)
            case_dir = op.join(self.output_dir, name)
            self.cases_dirs.append(case_dir)
            os.makedirs(case_dir, exist_ok=True)
            context.update(self.fixed_parameters)

    def build_case(self, case_context: dict, case_dir: str) -> None:
        pass

    def build_case_and_render_files(self, case_context: dict, case_dir: str) -> None:
        self.build_case(case_context, case_dir)
        for t_name in self.templates_name:
            try:
                template = self.env.get_template(t_name)
                rendered = template.render(case_context)
                with open(op.join(case_dir, t_name), "w") as f:
                    f.write(rendered)
            except Exception:
                copy_files(op.join(self.templates_dir, t_name), op.join(case_dir, t_name))

    def build_cases(
        self,
        mode: str = "one_by_one",
        cases_name_format: Callable = lambda ctx: f"{ctx.get('case_num'):04}",
        num_workers: Optional[int] = None,
    ) -> None:
        if self.cases_context is None:
            self.load_cases(mode=mode, cases_name_format=cases_name_format)
        
        workers = num_workers or self.num_workers
        if workers > 1:
            self.parallel_execute(
                self.build_case_and_render_files,
                list(zip(self.cases_context, self.cases_dirs)),
                workers,
            )
        else:
            for ctx, d in zip(self.cases_context, self.cases_dirs):
                self.build_case_and_render_files(ctx, d)

    def run_case(self, case_dir: str, launcher: str) -> None:
        cmd = self.available_launchers.get(launcher, launcher)
        subprocess.run(cmd, shell=True, cwd=case_dir, check=True)

    def run_cases(self, launcher: str, num_workers: Optional[int] = None) -> None:
        workers = num_workers or self.num_workers
        if workers > 1:
            self.parallel_execute(self.run_case, self.cases_dirs, workers, launcher=launcher)
        else:
            for d in self.cases_dirs:
                self.run_case(d, launcher)
