import os
import sys
from loguru import logger
from typing import Tuple, List, Dict, Callable
import subprocess
import signal
import sys
import time


def is_python_code_dir(xdir: str) -> bool:
    python_test_dir = os.path.join(xdir, "__pycache__")
    if os.path.exists(python_test_dir): return True
    return False


def push(git_dir: str, commit_msg: str):
    """ run git add/commit/push """

    logger.info("=====================================================================")
    logger.info(f"==== | run: pushd {git_dir} && git commit -a -m \"{commit_msg}\" && git push && popd")
    logger.info("=====================================================================")

    wd = os.getcwd()
    os.chdir(git_dir)
    try:
        if not os.path.exists(".git"): raise Exception("Missing .git directory")

        
        logger.info(f"git add --all .")
        subprocess.call(["git", "add", "--all", "."])
        logger.info(f"git commit -m {commit_msg}")
        subprocess.call(["git", "commit", "-m", commit_msg])
        logger.info("git push")
        subprocess.call(["git", "push"])

        logger.info("==== done")
    finally:
        os.chdir(wd)


def isbehind(git_dir: str = ".") -> bool:
    """ check if repo is behind origin """

    logger.info("=====================================================================")
    logger.info(f"==== |  run: pushd {git_dir} && git fetch && git status && popd")
    logger.info("=====================================================================")
 
    def read_output(p):
        result = []
        while True:
            rc = p.poll()
            if rc != None: break
            s = p.stdout.readline()
            if s == b"": continue
            s = s.decode()
            print(s)
            result.append(s)
        if rc != 0:
            raise Exception(f"return code was {rc}")
        return result

    wd = os.getcwd()
    os.chdir(git_dir)
    try:
        if not os.path.exists(".git"): raise Exception("Missing .git directory")

        logger.info("git fetch")
        subprocess.call(["git", "fetch"])

        logger.info("git status")
        p = subprocess.Popen(["git", "status"], stdout=subprocess.PIPE)
        lines = read_output(p)

        is_behind = False
        for x in lines:
            if x.startswith("Your branch is behind"): 
                is_behind = True

        logger.info("==== done")
        return is_behind
    finally:
        os.chdir(wd)

    
def pull(git_dir: str = "."):
    """ run git pull """

    logger.info("=====================================================================")
    logger.info(f"==== |  run: pushd {git_dir} && git pull && popd")
    logger.info("=====================================================================")

    wd = os.getcwd()
    os.chdir(git_dir)
    try:
        if not os.path.exists(".git"): raise Exception("Missing .git directory")

        logger.info("git pull")
        subprocess.call(["git", "pull"])
        logger.info("==== done")
    finally:
        os.chdir(wd)    


def monitor_start(key_word: str):
    """ monitor for source changes """
    args = ["python.exe"]
    fnd = False
    for x in sys.argv:
        if x == "python.exe": continue
        if x == key_word: 
            fnd = True
            continue
        args.append(x)

    if not fnd:
        raise Exception(f"Invalid call to monitor_start from subprocess")

    args.append("--guarded")

    logger.info("starting subprocess...")

    def signal_handler(signal, frame):
        print("^C")
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        rc = subprocess.call(args)        
        if rc != 0:
            logger.error(f"exit because return code = {rc}")
            break
        else:
            logger.warning("restarting in 10 seconds")
            time.sleep(10)


def monitor_check() -> bool:
    """ check for source """
    for x in sys.argv[1:]:
        if x == "--guarded":
            logger.info("check for source changes...")
            if isbehind(): 
                logger.info("pull new sources...")
                pull()
                return True 
    return False
