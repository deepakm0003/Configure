"""
main.py – Pipeline entrypoint.

Reads RS_ROLE and dispatches to the appropriate stage.
Run as: python -m pipeline.main
"""

import sys
from pipeline.rs_common.config import load
from pipeline.rs_common.metrics import log_json


def main() -> None:
    cfg = load()
    role = cfg.role.lower()
    log_json(role, "INFO", "pipeline_start", role=role)

    if role == "normalize":
        from pipeline.stages.normalize import run
    elif role == "detect":
        from pipeline.stages.detect import run
    elif role == "fuse":
        from pipeline.stages.fuse import run
    elif role == "alert":
        from pipeline.stages.alert import run
    else:
        log_json("main", "ERROR", "unknown_role", role=role,
                 valid=["normalize", "detect", "fuse", "alert"])
        sys.exit(1)

    run(cfg)


if __name__ == "__main__":
    main()
