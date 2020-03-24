import os
import sys
from loguru import logger

# can't put this in util because it needs to rerun before submodules are loaded.

def check_path():
    " confirm that the path/python path is set so absolute imports work "

    src_dir = os.path.dirname(__file__)

    path = os.environ.get("PATH")
    dirs = path.split(";")
    for d in dirs:
        if os.path.isdir(d) and os.path.samefile(d, src_dir):
            logger.debug("found src_dir in PATH")
            return

    path = os.environ.get("PYTHONPATH")
    if path != None:
        dirs = path.split(";")
        for d in dirs:
            if os.path.isdir(d) and os.path.samefile(d, src_dir):
                logger.debug("found src_dir in PYTHONPATH")
                return

    logger.error(f"Source dir ({src_dir}) must be in PATH or PYTHONPATH")
    exit(-1)
