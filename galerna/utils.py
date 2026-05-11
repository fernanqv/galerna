import logging
import os
import shutil


def get_simple_logger(
    name: str,
    level: str = "INFO",
    log_file: str | None = None,
    console: bool = True,
    console_format: str = "%(message)s",
    file_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
) -> logging.Logger:
    """
    Creates a simple logger that outputs to console and optionally to a file.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    # Console handler
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(console_format))
        logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(file_format))
        logger.addHandler(file_handler)

    return logger


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
