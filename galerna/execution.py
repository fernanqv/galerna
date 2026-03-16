import concurrent.futures
import io
import logging
import os
import os.path as op
import subprocess
import sys
from typing import Any, Callable, Dict, List, Optional


def parallel_execute(
    self,
    func: Callable,
    items: List[Any],
    num_workers: int,
    cpu_intensive: bool = False,
    **kwargs,
) -> Dict[int, Any]:
    """
    Execute a function in parallel across multiple items.

    Parameters
    ----------
    func : Callable
        The function to execute. Should accept single item and **kwargs.
    items : List[Any]
        List of items to process in parallel.
    num_workers : int
        Number of parallel workers to use.
    cpu_intensive : bool, optional
        If True, uses ProcessPoolExecutor, otherwise ThreadPoolExecutor.
        Default is False.
    **kwargs : dict
        Additional keyword arguments passed to func.

    Returns
    -------
    Dict[int, Any]
        Dictionary mapping item indices to function results.

    Raises
    ------
    Exception
        Any exception raised by func is logged and the job continues.

    Notes
    -----
    - Uses ThreadPoolExecutor for I/O-bound tasks
    - Uses ProcessPoolExecutor for CPU-bound tasks
    - Results maintain original item order via index mapping
    - Failed jobs are logged but don't stop execution

    Warnings
    --------
    - ThreadPoolExecutor may have GIL limitations
    - ProcessPoolExecutor doesn't work with non-picklable objects
    - File operations may fail with ThreadPoolExecutor

    Examples
    --------
    >>> def square(x):
    ...     return x * x
    >>> model = BlueMathModel()
    >>> results = model.parallel_execute(square, [1, 2, 3], num_workers=2)
    >>> print(results)
    {0: 1, 1: 4, 2: 9}
    """

    results = {}

    executor_class = ProcessPoolExecutor if cpu_intensive else ThreadPoolExecutor
    self.logger.info(f"Using {executor_class.__name__} for parallel execution")

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
                result = future.result()
                results[i] = result
            except Exception as exc:
                self.logger.error(f"Job for {i} generated an exception: {exc}")

    return results

def exec_bash_command(
    cmd: str,
    cwd: str,
    stdout_log: Optional[str] = None,
    stderr_log: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    log_output: bool = False
) -> None:
    """
    Execute a bash command with optional log redirection, merging, and logging output.
    """

    if logger:
        logger.debug(f"Executing command: {cmd} in {cwd}")
    
    actual_stdout = None
    actual_stderr = None
    
    if stdout_log:
        out_path = op.join(cwd, stdout_log)
        actual_stdout = open(out_path, "w")
    
    if stderr_log:
        if stderr_log == stdout_log:
            actual_stderr = subprocess.STDOUT
        else:
            err_path = op.join(cwd, stderr_log)
            actual_stderr = open(err_path, "w")
    
    try:
        # If log_output is requested, we need to capture output to process it
        stdout_pipe = subprocess.PIPE if log_output else actual_stdout
        stderr_pipe = subprocess.PIPE if log_output and actual_stderr != subprocess.STDOUT else actual_stderr
        
        process = subprocess.Popen(
            cmd, 
            shell=True, 
            cwd=cwd, 
            stdout=stdout_pipe,
            stderr=stderr_pipe,
            text=True, # Decode as strings
            bufsize=1  # Line buffered
        )

        if log_output:
            # We need to read from stdout (and potentially stderr) and log it while also writing to the file
            # If actual_stderr is STDOUT, stderr is merged into stdout
            
            import select
            
            streams = []
            if process.stdout:
                streams.append(process.stdout)
            if process.stderr:
                streams.append(process.stderr)
                
            while streams:
                readable, _, _ = select.select(streams, [], [])
                for stream in readable:
                    line = stream.readline()
                    if not line:
                        streams.remove(stream)
                        continue
                    
                    line_stripped = line.rstrip('\n')
                    if logger:
                        if stream is process.stderr and actual_stderr != subprocess.STDOUT:
                            logger.error(line_stripped)
                        else:
                            logger.info("Running command: %s: %s", cmd, line_stripped)
                    else:
                        sys.stdout.write(line)
                        sys.stdout.flush()
                    
                    if stream is process.stdout and actual_stdout:
                        actual_stdout.write(line)
                        actual_stdout.flush()
                    elif stream is process.stderr and actual_stderr and actual_stderr != subprocess.STDOUT:
                        actual_stderr.write(line)
                        actual_stderr.flush()
            
        process.wait()
        
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)
            
    except subprocess.CalledProcessError as e:
        if logger:
            logger.error(f"Command failed in {cwd}: {e}")
        raise
    except Exception as e:
        if logger:
            logger.error(f"Unexpected error executing command in {cwd}: {e}")
        raise
    finally:
        if isinstance(actual_stdout, io.IOBase):
            actual_stdout.close()
        if isinstance(actual_stderr, io.IOBase):
            actual_stderr.close()

def parallel_execute(
    func: Callable,
    items: List[Any],
    num_workers: int,
    logger: Optional[logging.Logger] = None,
    **kwargs
) -> List[Any]:
    """
    Helper function to execute a function in parallel using a ThreadPoolExecutor.
    """
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(func, item, **kwargs) for item in items]
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                if logger:
                    logger.error(f"Parallel execution generated an exception: {exc}")
                results.append(exc)
    return results

def run_detached(func: Callable, *args, **kwargs) -> Optional[int]:
    """
    Run a function in a fully detached background process using the double-fork pattern.
    Only available on Unix-like systems.
    
    Returns
    -------
    Optional[int]
        The PID of the first child process, or None if on an unsupported OS (like Windows).
    """
    if not hasattr(os, "fork"):
        raise NotImplementedError("Detached execution is only supported on Unix-like OS with os.fork().")

    try:
        pid = os.fork()
        if pid > 0:
            # First parent returns
            os.waitpid(pid, 0)
            return pid
    except OSError as e:
        sys.stderr.write(f"Fork #1 failed: {e}\n")
        sys.exit(1)

    # --- We are now in the first child ---
    # Decouple from parent environment
    os.chdir("/")
    os.setsid()
    os.umask(0)

    # Second fork
    try:
        pid = os.fork()
        if pid > 0:
            # First child exits, making the second child an orphan adopted by init (PID 1)
            sys.exit(0)
    except OSError as e:
        sys.stderr.write(f"Fork #2 failed: {e}\n")
        sys.exit(1)

    # --- We are now in the second child (detached) ---
    # Redirect standard file descriptors to avoid hanging or broken pipes.
    sys.stdout.flush()
    sys.stderr.flush()
    try:
        with open(os.devnull, 'r') as f:
            os.dup2(f.fileno(), sys.stdin.fileno())
        with open(os.devnull, 'a') as f:
            os.dup2(f.fileno(), sys.stdout.fileno())
        with open(os.devnull, 'a') as f:
            os.dup2(f.fileno(), sys.stderr.fileno())
    except Exception:
        pass

    # Run the target function
    try:
        func(*args, **kwargs)
    except Exception as e:
        import traceback
        try:
            with open("/tmp/detached_err.txt", "w") as f:
                traceback.print_exc(file=f)
        except Exception:
            pass
    finally:
        sys.exit(0)
