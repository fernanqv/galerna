import itertools
import logging
import math
import os
import os.path as op
import json
from typing import Any, Callable, Dict, List, Optional, Union
from jinja2 import Environment, FileSystemLoader
from .execution import exec_bash_command
from .utils import copy_files, get_simple_logger      


class Galerna:
    """
    Base class for numerical models wrappers.
    Autonomous implementation without external core dependencies.

    Attributes
    ----------
    templates_dir : str
        The directory where the templates are searched.
    variable_parameters : dict or str
        The parameters to be used for all cases, or a path to a YAML file containing them.
    fixed_parameters : dict
        The fixed parameters for the cases.
    output_dir : str
        The directory where the output cases are saved.
    command : str
        Bash command string rendered with Jinja2 per case.
    """

    def __init__(
        self,
        templates_dir: Optional[str] = None,
        variable_parameters: Union[dict, str] = None,
        fixed_parameters: Optional[dict] = None,
        output_dir: str = "output",
        templates_name: Union[List[str], str] = "all",
        cases_name_format: Union[str, Callable] = '{{ "%04d" | format(case_num) }}',
        mode: str = "one_by_one",
        log_level: str = "INFO",
        log_file: Optional[str] = None,
        log_console: Optional[bool] = None,
        command: str = None,
        launcher_bulk: Optional[str] = None,
        sbatch_launcher: Optional[Union[bool, str]] = None,
        tasks_per_node: int = 1,
        max_workers: int = 1,
        sbatch_template: Optional[str] = False,
    ) -> None:
        """
        Initializes the Galerna instance.

        Parameters
        ----------
        templates_dir : str, optional
            The directory where the templates are searched. Default is None.
        variable_parameters : dict or str, optional
            The parameters to be used for all cases, or a path to a YAML file containing them. Default is empty dict.
        fixed_parameters : dict, optional
            The fixed parameters for the cases. Default is empty dict.
        output_dir : str, optional
            The directory where the output cases are saved. Default is 'output'.
        templates_name : Union[List[str], str], optional
            The names of the templates to use. Default is "all".
        cases_name_format : Union[str, Callable], optional
            The format for naming case directories (Jinja2 format). Default is '{{ "%04d" | format(case_num) }}'.
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
        command : str
            Bash command string rendered with Jinja2 per case.
        launcher_bulk : str, optional
            Bash command rendered with Jinja2 for bulk execution.¡ 
        tasks_per_node : int, optional
            Number of commands processed by each SLURM_ARRAY_TASK_ID. Default is 1000
        max_workers : int, optional
            Number of concurrent jobs executed by parallel locally on each node. Default is 40.
        sbatch_launcher : bool or str, optional
            If True or a valid path to a template, generates a SLURM array script for
             executing the cases in bulk on a cluster. Default is False."""
        
        if log_console is None:
            log_console = log_file is None

        self._logger = get_simple_logger(
            name=self.__class__.__name__, 
            level=log_level, 
            log_file=log_file,
            console=log_console
        )
        
        self.templates_dir = templates_dir
        if variable_parameters is None:
            self.variable_parameters = {}

        # Is variable_parameters is a string, we assume it is a path to a YAML file and we load it
        elif isinstance(variable_parameters, str):
            import yaml
            if not os.path.isfile(variable_parameters):
                raise FileNotFoundError(f"variable_parameters file not found: {variable_parameters}")
            with open(variable_parameters, "r") as f:
                self.variable_parameters = yaml.safe_load(f) or {}
        else:
            self.variable_parameters = variable_parameters

        self.fixed_parameters = fixed_parameters or {}
        self.output_dir = output_dir
        self.cases_name_format = cases_name_format
        self.mode = mode
        self.command = command
        self.launcher_bulk = launcher_bulk
        self.sbatch_launcher = sbatch_launcher
        self.tasks_per_node = tasks_per_node
        self.max_workers = max_workers

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
    
        # Save the cases context to a JSON file in the output directory for reference
        context_path = op.join(self.output_dir, "cases_context.json")
        os.makedirs(self.output_dir, exist_ok=True)
        with open(context_path, "w") as f:
            json.dump(self.cases_context, f, indent=4)
        self.logger.info(f"Cases context saved to {context_path}")


    def _generate_cases_context(self) -> None:
        """Generates the base cases context combinations and calculates directories."""
        # Convert any string "range(x, y)" into a list of integers
        for key, value in self.variable_parameters.items():
            if isinstance(value, str) and value.strip().startswith("range("):
                try:
                    self.variable_parameters[key] = list(eval(value.strip()))
                except Exception as e:
                    self.logger.warning(f"Could not evaluate range for parameter {key}: {e}")

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
                if self.env:
                    folder_name = self.env.from_string(self.cases_name_format).render(context)
                    command_cmd = self.env.from_string(self.command).render(context)
                else:
                    from jinja2 import Template
                    folder_name = Template(self.cases_name_format).render(context)
                    command_cmd = Template(self.command).render(context)
            else:
                folder_name = self.cases_name_format(context)

            if self.templates_dir is None:
                context["case_dir"] = op.abspath(self.output_dir)
            else:
                context["case_dir"] = op.abspath(op.join(self.output_dir, folder_name))
            
            context["command_cmd"] = command_cmd

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

        if self.templates_dir is not None:        
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
        output_log_file: str = "wrapper_out.log",
        error_log_file: str = "wrapper_error.log",
    ) -> None:
        """
        Run the case based on the command derived from the case context.

        Parameters
        ----------
        case_num : int
            The case number to run.
        output_log_file : str, optional
            The name of the output log file. Default is "wrapper_out.log".
        error_log_file : str, optional
            The name of the error log file. Default is "wrapper_error.log".
        """

        context = self.cases_context[case_num]
        
        if not context["command_cmd"]:
            raise ValueError("command is not defined. Please set it in __init__.")
        
        case_dir = context["case_dir"]
        command_cmd= context["command_cmd"]

        # Run the case in the case directory
        self.logger.info(f"Running case {case_num} in {case_dir} with command={command_cmd}")
        output_log_file = op.join(case_dir, output_log_file)

        exec_bash_command(
            cmd=command_cmd,
            cwd=case_dir,
            log_output=True,
            logger=self.logger,
        )

    def run_cases(
        self,
        cases: List[int] = None,
    ) -> None:
        """
        Run the cases sequentially.
        Cases to run can be specified.

        Parameters
        ----------
        cases : List[int], optional
            The list with the cases to run. Default is None.
        """

        if self.cases_context is None or self.cases_dirs is None:
            raise ValueError(
                "Cases context or cases directories are not set. Please run load_cases() first."
            )

        if cases is not None:
            self.logger.info(
                f"Cases to run was specified, so just {cases} will be run."
            )
            cases_list = cases
        else:
            cases_list = list(range(len(self.cases_dirs)))

        self.logger.debug(f"Running cases.")
        for case_num in cases_list:
            try:
                self.run_case(
                    case_num=case_num,
                )
            except Exception as exc:
                self.logger.error(
                    f"Job for case {case_num} generated an exception: {exc}."
                )

        self.logger.info("All cases executed.")

    def run_cases_bulk(
        self,
        path_to_execute: str = None,
    ) -> None:
        """
        Run the cases in bulk based on the launcher_bulk command.
        This is thought to be used in a cluster environment, as it is a bulk execution of the cases.
        By default, the command is executed in the output directory, where the cases are saved.

        Parameters
        ----------
        path_to_execute : str, optional
            The path to execute the command. Default is None (uses self.output_dir).

        Examples
        --------
        # This will execute the bulk launcher in the output directory.
        >>> wrapper.run_cases_bulk()
        # This will execute the bulk launcher in the specified path.
        >>> wrapper.run_cases_bulk(path_to_execute="/my/path/to/execute")
        """

        if not self.launcher_bulk:
            raise ValueError("launcher_bulk is not defined. Please set it in __init__.")

        if self.env:
            launcher_cmd = self.env.from_string(self.launcher_bulk).render(self.fixed_parameters)
        else:
            from jinja2 import Template
            launcher_cmd = Template(self.launcher_bulk).render(self.fixed_parameters)
                
        if path_to_execute is None:
            path_to_execute = self.output_dir

        self.logger.info(f"Running cases with launcher={launcher_cmd} in {path_to_execute}")
        exec_bash_command(cmd=launcher_cmd, cwd=path_to_execute, logger=self.logger)


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

        postprocessed_files = []
        for context in contexts_to_build:
            postprocessed_file = self.postprocess_case(context, overwrite=overwrite, clean_after=clean_after, **kwargs)
            postprocessed_files.append(postprocessed_file)
        return postprocessed_files
    
    def monitor_cases(self, **kwargs) -> None:
        """
        Monitor the cases execution. This can be implemented to check the status of the cases in a cluster environment, for example.
        All extra keyword arguments will be passed to the monitor_case method.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments to be passed to the monitor_case method.   
        """

        
