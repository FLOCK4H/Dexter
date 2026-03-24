from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TextIO


def _close_handler(handler: logging.Handler) -> None:
    try:
        handler.flush()
    except Exception:
        pass
    try:
        handler.close()
    except Exception:
        pass


def configure_root_logging(
    *,
    format: str,
    level: int = logging.INFO,
    datefmt: str | None = None,
    log_file: Path | str | None = None,
    stream: TextIO | None = None,
    force: bool = False,
) -> bool:
    root = logging.getLogger()
    if force:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            _close_handler(handler)
    elif root.handlers:
        return False

    if stream is None:
        stream = sys.stdout
    if stream is not None and hasattr(stream, "reconfigure"):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

    handlers: list[logging.Handler] = []
    if log_file is not None:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8", delay=True))
    if stream is not None:
        handlers.append(logging.StreamHandler(stream))

    logging.basicConfig(
        format=format,
        datefmt=datefmt,
        level=level,
        handlers=handlers,
    )
    return True
