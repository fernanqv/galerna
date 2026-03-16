import copy
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


class Galerna:
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
        num_workers: int = 1,
    ) -> None:
        """
        Initializes the Galerna instance.

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
        num_workers : int, optional
            The number of workers to use for parallel execution. Default is 1.
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
        self.num_workers = num_workers

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

    @property
    def cases_dirs(self) -> List[str]:
        """Returns a list of all case directories."""
        return [ctx.get("case_dir") for ctx in self.cases_context] if self.cases_context else []


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
        case_num: int,
        launcher: str,
        output_log_file: str = "wrapper_out.log",
        error_log_file: str = "wrapper_error.log",
    ) -> None:
        """
        Run the case based on the launcher specified.

        Parameters
        ----------
        case_num : int
            The case number to run.
        launcher : str
            The launcher to run the case.
        output_log_file : str, optional
            The name of the output log file. Default is "wrapper_out.log".
        error_log_file : str, optional
            The name of the error log file. Default is "wrapper_error.log".
        """

        # Get launcher command from the available launchers
        launcher = self.available_launchers.get(launcher, launcher)
        case_dir = self.cases_dirs[case_num]

        # Run the case in the case directory
        self.logger.info(f"Running case {case_num} in {case_dir} with launcher={launcher}.")
        output_log_file = op.join(case_dir, output_log_file)

        exec_bash_command(
            cmd=launcher,
            cwd=case_dir,
            log_output=True,
            logger=self.logger,
        )

    def run_cases(
        self,
        launcher: str,
        cases_to_run: List[int] = None,
        num_workers: int = None,
    ) -> None:
        """
        Run the cases based on the launcher specified.
        Cases to run can be specified.
        Parallel execution is optional by modifying the num_workers parameter.

        Parameters
        ----------
        launcher : str
            The launcher to run the cases.
        cases_to_run : List[int], optional
            The list with the cases to run. Default is None.
        num_workers : int, optional
            The number of parallel workers. Default is None.
        """

        if self.cases_context is None or self.cases_dirs is None:
            raise ValueError(
                "Cases context or cases directories are not set. Please run load_cases() first."
            )

        if num_workers is None:
            num_workers = self.num_workers

        # Get launcher command from the available launchers
        launcher = self.available_launchers.get(launcher, launcher)

        if cases_to_run is not None:
            self.logger.warning(
                f"Cases to run was specified, so just {cases_to_run} will be run."
            )
            cases_to_run_list = cases_to_run
        else:
            cases_to_run_list = list(range(len(self.cases_dirs)))

        if num_workers > 1:
            self.logger.debug(
                f"Running cases in parallel with launcher={launcher}. Number of workers: {num_workers}."
            )
            _results = parallel_execute(
                func=self.run_case,
                items=cases_to_run_list,
                num_workers=num_workers,
                logger=self.logger,
                launcher=launcher,
            )
        else:
            self.logger.debug(f"Running cases sequentially with launcher={launcher}.")
            for case_num in cases_to_run_list:
                try:
                    self.run_case(
                        case_num=case_num,
                        launcher=launcher,
                    )
                except Exception as exc:
                    self.logger.error(
                        f"Job for case {case_num} generated an exception: {exc}."
                    )

        self.logger.info("All cases executed.")

    def _run_cases_with_status(
        self,
        launcher: str,
        cases_to_run: List[int],
        num_workers: int,
        status_queue: queue.Queue,
    ) -> None:
        """
        Run the cases and update the status queue.

        Parameters
        ----------
        launcher : str
            The launcher to run the cases.
        cases_to_run : List[int]
            The list with the cases to run.
        num_workers : int
            The number of parallel workers.
        status_queue : Queue
            The queue to update the status.
        """

        try:
            self.run_cases(launcher, cases_to_run, num_workers)
            status_queue.put("Completed")
        except Exception as e:
            status_queue.put(f"Error: {e}")

    def run_cases_in_background(
        self,
        launcher: str,
        cases_to_run: List[int] = None,
        num_workers: int = None,
        detached: bool = False,
    ) -> None:
        """
        Run the cases in the background based on the launcher specified.
        Cases to run can be specified.
        Parallel execution is optional by modifying the num_workers parameter.

        Parameters
        ----------
        launcher : str
            The launcher to run the cases.
        cases_to_run : List[int], optional
            The list with the cases to run. Default is None.
        num_workers : int, optional
            The number of parallel workers. Default is None.
        detached : bool, optional
            If True, runs the process completely detached from the parent.
            If False, runs in a background thread of the parent process. Default is False.
        """

        if num_workers is None:
            num_workers = self.num_workers

        if detached:
            from .execution import run_detached
            self.logger.info("Running cases in a fully detached background process.")
            run_detached(self.run_cases, launcher, cases_to_run, num_workers)
        else:
            if not hasattr(self, "status_queue") or self.status_queue is None:
                self.status_queue = queue.Queue()
            self.thread = threading.Thread(
                target=self._run_cases_with_status,
                args=(launcher, cases_to_run, num_workers, self.status_queue),
            )
            self.thread.start()

    def get_thread_status(self) -> str:
        """
        Get the status of the background thread.

        Returns
        -------
        str
            The status of the background thread.
        """

        if self.thread is None:
            return "Not started"
        elif self.thread.is_alive():
            return "Running"
        else:
            return self.status_queue.get()

    def run_cases_bulk(
        self,
        launcher: str,
        path_to_execute: str = None,
    ) -> None:
        """
        Run the cases based on the launcher specified.
        This is thought to be used in a cluster environment, as it is a bulk execution of the cases.
        By default, the command is executed in the output directory, where the cases are saved,
        and where the example sbatch file is saved.

        Parameters
        ----------
        launcher : str
            The launcher to run the cases.
        path_to_execute : str, optional
            The path to execute the command. Default is None.

        Examples
        --------
        # This will execute the specified launcher in the output directory.
        >>> wrapper.run_cases_bulk(launcher="sbatch sbatch_example.sh")
        # This will execute the specified launcher in the specified path.
        >>> wrapper.run_cases_bulk(launcher="my_launcher.sh", path_to_execute="/my/path/to/execute")
        """

        if path_to_execute is None:
            path_to_execute = self.output_dir

        self.logger.info(f"Running cases with launcher={launcher} in {path_to_execute}")
        exec_bash_command(cmd=launcher, cwd=path_to_execute, logger=self.logger)

    def monitor_cases(
        self, cases_status: dict, value_counts: str
    ) -> Union["pd.DataFrame", dict]:
        """
        Return the status of the cases.
        This method is used to monitor the cases and log relevant information.
        It is called in the child class to monitor the cases.

        Parameters
        ----------
        cases_status : dict
            The dictionary with the cases status.
            Each key is the base case directory name and the value is the status of the case.
            This status can be any string.
        value_counts : str, optional
            The value counts to be returned.
            If "simple", it returns a dictionary with the number of cases in each status.
            If "percentage", it returns a DataFrame with the percentage of cases in each status.
            If "cases", it returns a dictionary with the cases in each status.
            Default is None.

        Returns
        -------
        Union[pd.DataFrame, dict]
            The cases status as a pandas DataFrame or a dictionary with aggregated info.
        """

        full_monitorization_df = pd.DataFrame(
            cases_status.items(), columns=["Case", "Status"]
        )
        if value_counts:
            value_counts_df = full_monitorization_df.set_index("Case").value_counts()
            if value_counts == "simple":
                return value_counts_df
            elif value_counts == "percentage":
                return value_counts_df / len(full_monitorization_df) * 100
            value_counts_unique_values = [
                run_type[0] for run_type in value_counts_df.index.values
            ]
            value_counts_dict = {
                run_type: list(
                    full_monitorization_df.where(
                        full_monitorization_df["Status"] == run_type
                    )
                    .dropna()["Case"]
                    .values
                )
                for run_type in value_counts_unique_values
            }
            return value_counts_dict
        else:
            return full_monitorization_df

    def postprocess_case(self, **kwargs) -> None:
        """
        Postprocess the model output.
        """

        raise NotImplementedError("The method postprocess_case must be implemented.")

    def postprocess_cases(
        self,
        cases: List[int] = None,
        clean_after: bool = False,
        overwrite: bool = False,
        **kwargs,
    ) -> None:
        """
        Postprocess the model output.
        All extra keyword arguments will be passed to the postprocess_case method.

        Parameters
        ----------
        cases : List[int], optional
            The list with the cases to postprocess. Default is None.
        clean_after : bool, optional
            Clean the cases directories after postprocessing. Default is False.
        overwrite : bool, optional
            Overwrite the postprocessed file if it exists. Default is False.
        **kwargs
            Additional keyword arguments to be passed to the postprocess_case method.

        """

        if cases is not None:
            contexts_to_build = [self.cases_context[i] for i in cases]
        else:
            contexts_to_build = self.cases_context

        self.logger.debug(f"Starting to build {len(contexts_to_build)} cases.")
        for context in contexts_to_build:
            self.postprocess_case(context, overwrite=overwrite, clean_after=clean_after, **kwargs)
