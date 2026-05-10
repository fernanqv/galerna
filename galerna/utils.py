import io
import logging
import os
import os.path as op
import shutil
import subprocess
import sys


def get_simple_logger(
    name: str,
    level: str = "INFO",
    log_file: str | None = None,
    console: bool = True,
) -> logging.Logger:
    """
    Creates a simple logger that outputs to console and optionally to a file.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding handlers if they already exist
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Console handler
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def write_array_in_file(array, filename: str) -> None:
    """
    Write a numpy-like array to a file.
    """
    with open(filename, "w") as f:
        if hasattr(array, "ndim") and array.ndim == 2:
            for row in array:
                f.write(" ".join(map(str, row)) + "\n")
        else:
            for item in array:
                f.write(f"{item}\n")


def copy_files(src: str, dst: str) -> None:
    """
    Copy file(s) from source to destination.
    """
    if os.path.isdir(src):
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
    else:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)


def exec_bash_command(
    cmd: str,
    cwd: str,
    stdout_log: str | None = None,
    stderr_log: str | None = None,
    logger: logging.Logger | None = None,
    log_output: bool = False,
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
        stderr_pipe = (
            subprocess.PIPE
            if log_output and actual_stderr != subprocess.STDOUT
            else actual_stderr
        )

        process = subprocess.Popen(
            cmd,
            shell=True,
            cwd=cwd,
            stdout=stdout_pipe,
            stderr=stderr_pipe,
            text=True,
            bufsize=1,
        )

        if log_output:
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

                    line_stripped = line.rstrip("\n")
                    if logger:
                        if (
                            stream is process.stderr
                            and actual_stderr != subprocess.STDOUT
                        ):
                            logger.error(line_stripped)
                        else:
                            logger.info("Running command: %s: %s", cmd, line_stripped)
                    else:
                        sys.stdout.write(line)
                        sys.stdout.flush()

                    if stream is process.stdout and actual_stdout:
                        actual_stdout.write(line)
                        actual_stdout.flush()
                    elif (
                        stream is process.stderr
                        and actual_stderr
                        and actual_stderr != subprocess.STDOUT
                    ):
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


def create_command_line(
    cases, cases_context: dict[str, dict], run_command_file: str
) -> None:
    with open(run_command_file, "w") as f:
        for case in cases:
            case_context = cases_context[case]
            f.write(f"cd {case_context['case_dir']}; {case_context['command_cmd']}\n")
