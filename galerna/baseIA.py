import io
import itertools
import logging
import os
import os.path as op
import queue
import subprocess
import sys
import threading
from typing import Any, Callable, Dict, List, Optional, Union

from jinja2 import Environment, FileSystemLoader

from .execution import exec_bash_command, parallel_execute
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
    """

    available_launchers = {}

    def __init__(
        self,
        templates_dir: Optional[str],
        variable_parameters: dict,
        fixed_parameters: dict,
        output_dir: str,
        templates_name: Union[List[str], str] = "all",
        cases_name_format: Union[str, Callable] = "{case_num:04}",
        mode: str = "one_by_one",
        log_level: str = "INFO",
        log_file: Optional[str] = None,
        log_console: Optional[bool] = None,
    ) -> None:
        """
        Initializes the ModelWrapper.

        Parameters
        ----------
        templates_dir : str, optional
            The directory where the templates are searched.
        variable_parameters : dict
            The parameters to be used for all cases.
        fixed_parameters : dict
            The fixed parameters for the cases.
        output_dir : str
            The directory where the output cases are saved.
        templates_name : Union[List[str], str], optional
            The names of the templates to use. Default is "all".
        cases_name_format : Union[str, Callable], optional
            The format for naming case directories. Default is "{case_num:04}".
        mode : str, optional
            The mode to load the cases. Can be "all_combinations" or "one_by_one".
            Default is "one_by_one".
        log_level : str, optional
            The logging level (e.g., "DEBUG", "INFO", "WARNING"). Default is "INFO".
        log_file : str, optional
            Path to a file where logs should be written. If None, only console output is used.
        log_console : bool, optional
            Whether to output logs to the console. 
            If None, it defaults to True if log_file is None, and False otherwise.
        """
        if log_console is None:
            log_console = log_file is None

        self._logger = get_simple_logger(
            name=self.__class__.__name__, 
            level=log_level, 
            log_file=log_file,
            console=log_console
        )
        
        self.templates_dir = templates_dir
        self.variable_parameters = variable_parameters
        self.fixed_parameters = fixed_parameters
        self.output_dir = output_dir
        self.cases_name_format = cases_name_format
        self.mode = mode

        if self.templates_dir is not None:
            if not os.path.isdir(self.templates_dir):
                raise FileNotFoundError(f"Template directory not found: {self.templates_dir}")
            self._env = Environment(loader=FileSystemLoader(self.templates_dir))
            if templates_name == "all":
                self.templates_name = self.env.list_templates()
            else:
                self.templates_name = templates_name
        else:
            self._env = None
            self.templates_name = []

        self.cases_context: List[dict] = []
        self._generate_cases_context()

        self._thread: Optional[threading.Thread] = None
        self._status_queue: Optional[queue.Queue] = None
        self.num_workers = 1

    def _generate_cases_context(self) -> None:
        """Generates the base cases context combinations and calculates directories."""
        if self.mode == "all_combinations":
            keys = self.variable_parameters.keys()
            values = self.variable_parameters.values()
            combinations = itertools.product(*values)
            self.cases_context = [dict(zip(keys, c)) for c in combinations]
        elif self.mode == "one_by_one":
            num_cases = len(next(iter(self.variable_parameters.values())))
            self.cases_context = []
            for i in range(num_cases):
                case = {p: v[i] for p, v in self.variable_parameters.items()}
                self.cases_context.append(case)
        else:
            raise ValueError(f"Invalid mode: {self.mode}")

        self.logger.debug(f"Generated {len(self.cases_context)} cases in mode '{self.mode}'.")

        for i, context in enumerate(self.cases_context):
            context["case_num"] = i
            context.update(self.fixed_parameters)
            
            # Calculate case directory
            if isinstance(self.cases_name_format, str):
                name = self.cases_name_format.format(**context)
            else:
                name = self.cases_name_format(context)
            context["case_dir"] = op.abspath(op.join(self.output_dir, name))

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


    def build_case(self, case_context: dict) -> None:
        """
        Custom logic to build a specific case. 
        This method should be overridden by subclasses.

        Parameters
        ----------
        case_context : dict
            The context (parameters) for this specific case.
        """
        pass

    def get_context(self) -> Union[List[dict], Any]:
        """
        Returns the cases context.
        If pandas is installed, it returns a DataFrame.
        Otherwise, it returns a list of dictionaries.

        Returns
        -------
        Union[List[dict], pd.DataFrame]
            The cases context.
        """
        try:
            import pandas as pd
            return pd.DataFrame(self.cases_context)
        except ImportError:
            return self.cases_context

    def build_case_and_render_files(self, case_context: dict) -> None:
        """
        Creates the case directory, calls build_case, and renders templates.

        Parameters
        ----------
        case_context : dict
            The context (parameters) for this specific case.
        """
        case_dir = case_context["case_dir"]
        self.logger.debug(f"Building case {case_context.get('case_num')} in {case_dir}")
        os.makedirs(case_dir, exist_ok=True)
        self.build_case(case_context)
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
        cases: Optional[List[int]] = None,
    ) -> None:
        """
        Builds the selected cases by creating directories, calling build_case,
        and rendering template files.

        Parameters
        ----------
        cases : List[int], optional
            A list of indices of the cases to build.
            If None, all loaded cases are built.
        """
        
        if cases is not None:
            contexts_to_build = [self.cases_context[i] for i in cases]
        else:
            contexts_to_build = self.cases_context

        self.logger.debug(f"Starting to build {len(contexts_to_build)} cases.")
        for context in contexts_to_build:
            self.build_case_and_render_files(context)

    def run_case(
        self,
        case_dir: str,
        launcher: str,
        stdout_log: Optional[str] = "wrapper_out.log",
        stderr_log: Optional[str] = "wrapper_error.log",
        **kwargs
    ) -> None:
        """
        Run a single case based on the launcher specified.

        Parameters
        ----------
        case_dir : str
            The case directory.
        launcher : str
            The launcher command or key from available_launchers.
        stdout_log : str, optional
            The name of the file to redirect standard output. Default is "wrapper_out.log".
        stderr_log : str, optional
            The name of the file to redirect standard error. Default is "wrapper_error.log".
            If same as stdout_log, stderr will be merged.
        """
        # Get launcher command from the available launchers if it's a key
        cmd_template = self.available_launchers.get(launcher, launcher)
        
        # We need the context for this case to format the command
        # This is a bit inefficient if called in a loop, but clean for single usage
        context = next((c for c in self.cases_context if c["case_dir"] == case_dir), {})
        
        try:
            cmd = cmd_template.format(**context)
        except (KeyError, IndexError):
            cmd = cmd_template

        self.logger.info(f"Running case in {case_dir} with cmd='{cmd}'.")
        
        try:
            exec_bash_command(
                cmd=cmd,
                cwd=case_dir,
                stdout_log=stdout_log,
                stderr_log=stderr_log,
                logger=self.logger
            )
            self.logger.info(f"Finished running case in {case_dir}.")
        except Exception as e:
            self.logger.error(f"Failed to run case in {case_dir}: {e}")
            raise

    def run_cases(
        self, 
        launcher: str, 
        cases_to_run: Optional[List[int]] = None,
        num_workers: Optional[int] = None,
        stdout_log: Optional[str] = "wrapper_out.log", 
        stderr_log: Optional[str] = "wrapper_error.log"
    ) -> None:
        """
        Runs selected cases using the provided launcher.

        Parameters
        ----------
        launcher : str
            The launcher command.
        cases_to_run : List[int], optional
            Indices of the cases to run. If None, all cases are run.
        num_workers : int, optional
            Number of parallel workers. If None, uses self.num_workers.
        stdout_log : str, optional
            The name of the file to redirect standard output.
        stderr_log : str, optional
            The name of the file to redirect standard error.
        """
        if num_workers is None:
            num_workers = self.num_workers

        if cases_to_run is not None:
            contexts_to_run = [self.cases_context[i] for i in cases_to_run]
        else:
            contexts_to_run = self.cases_context

        case_dirs = [c["case_dir"] for c in contexts_to_run]
        
        for d in case_dirs:
            os.makedirs(d, exist_ok=True)

        self.logger.info(f"Running {len(contexts_to_run)} cases (workers={num_workers}).")

        if num_workers > 1:
            parallel_execute(
                func=self.run_case,
                items=case_dirs,
                num_workers=num_workers,
                logger=self.logger,
                launcher=launcher,
                stdout_log=stdout_log,
                stderr_log=stderr_log
            )
        else:
            for case_dir in case_dirs:
                try:
                    self.run_case(
                        case_dir=case_dir,
                        launcher=launcher,
                        stdout_log=stdout_log,
                        stderr_log=stderr_log
                    )
                except Exception:
                    # Logs are handled in run_case
                    continue

        self.logger.info("All cases executed.")

    def _run_cases_with_status_update(self, *args, **kwargs) -> None:
        """Helper for background execution to update status queue."""
        try:
            self.run_cases(*args, **kwargs)
            if self._status_queue:
                self._status_queue.put("Completed")
            self.logger.info("Background execution completed successfully.")
        except Exception as e:
            self.logger.error(f"Background execution failed: {e}", exc_info=True)
            if self._status_queue:
                self._status_queue.put(f"Error: {e}")

    def run_cases_in_background(
        self,
        launcher: str,
        cases_to_run: Optional[List[int]] = None,
        num_workers: Optional[int] = None,
        stdout_log: Optional[str] = "wrapper_out.log",
        stderr_log: Optional[str] = "wrapper_error.log",
        detached: bool = False
    ) -> None:
        """
        Run the cases in the background.
        
        Parameters
        ----------
        launcher : str
            The launcher command.
        cases_to_run : List[int], optional
            Indices of the cases to run.
        num_workers : int, optional
            Number of parallel workers.
        stdout_log : str, optional
            The name of the file to redirect standard output.
        stderr_log : str, optional
            The name of the file to redirect standard error.
        detached : bool, optional
            If True, use double-fork to detach the process from the parent.
            Only supported on Unix-like systems. Default is False (uses a Thread).
        """
        if detached and os.name != "nt":
            self.logger.info("Starting detached background execution (double-fork).")
            try:
                pid = os.fork()
                if pid > 0:
                    # Parent process returns immediately
                    return
            except OSError as e:
                self.logger.error(f"First fork failed: {e}")
                raise

            # First child process
            os.setsid()
            try:
                pid = os.fork()
                if pid > 0:
                    # First child exits
                    sys.exit(0)
            except OSError as e:
                self.logger.error(f"Second fork failed: {e}")
                sys.exit(1)

            # Second child (daemon process)
            # Close standard file descriptors to truly detach
            sys.stdout.flush()
            sys.stderr.flush()
            with open(os.devnull, "r") as f:
                os.dup2(f.fileno(), sys.stdin.fileno())
            with open(os.devnull, "a+") as f:
                os.dup2(f.fileno(), sys.stdout.fileno())
                os.dup2(f.fileno(), sys.stderr.fileno())

            # Run the cases
            try:
                self.run_cases(launcher, cases_to_run, num_workers, stdout_log, stderr_log)
                sys.exit(0)
            except Exception as e:
                # Since we redirected stdout/stderr to devnull, logs are our only hope
                self.logger.critical(f"Detached process failed: {e}", exc_info=True)
                sys.exit(1)
        else:
            if detached:
                self.logger.warning("Detached mode is not supported on Windows. Falling back to Thread.")
            
            self._status_queue = queue.Queue()
            self._thread = threading.Thread(
                target=self._run_cases_with_status_update,
                args=(launcher, cases_to_run, num_workers, stdout_log, stderr_log)
            )
            self._thread.start()
            self.logger.info("Started background execution thread.")

    def get_thread_status(self) -> str:
        """
        Get the status of the background thread.
        """
        if self._thread is None:
            return "Not started"
        if self._thread.is_alive():
            return "Running"
        
        try:
            return self._status_queue.get_nowait()
        except (queue.Empty, AttributeError):
            return "Finished"

    def run_cases_bulk(
        self,
        launcher: str,
        path_to_execute: Optional[str] = None,
    ) -> None:
        """
        Run cases in bulk (e.g., submitting a single sbatch script).
        """
        if path_to_execute is None:
            path_to_execute = self.output_dir

        self.logger.info(f"Running cases bulk with launcher='{launcher}' in {path_to_execute}")
        exec_bash_command(stdout_log=None, stderr_log=None, cmd=launcher, cwd=path_to_execute, logger=self.logger)
