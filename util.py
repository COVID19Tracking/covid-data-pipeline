import os
import sys
import requests
from datetime import datetime, timezone, timedelta
import time
import pytz
from loguru import logger
import re
from typing import Tuple, List, Dict, Callable
import subprocess
import signal
import sys

from requests.packages import urllib3
urllib3.disable_warnings() 

def fetch(page: str) -> [bytes, int]:
    #print(f"fetch {page}")
    try:
        resp = requests.get(page, verify=False, timeout=30)
        return resp.content, resp.status_code
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return None, 999

def is_bad_content(content: bytes) -> [bool, str]:

    if content == None: return True, "Empty Response"
    if len(content) < 600: return True, f"Response is {len(content)} bytes"
    if re.search(b"Request unsuccessful. Incapsula incident", content):
        return True, f"Site uses Incapsula"
    return False, None


def file_age(xpath: str) -> float:
    """ get age of a file in minutes """

    #print(xpath)
    mtime = os.path.getmtime(xpath)
    mtime = datetime.fromtimestamp(mtime)

    xnow = datetime.now()
    xdelta = (xnow - mtime).seconds / 60.0

    return xdelta

def format_mins(x : float):
    if x < 60.0:
        return f"{x:.0f} mins"
    x /= 60.0
    if x < 24.0:
        return f"{x:.1f} hours"
    return f"{x:.1f} days"

def format_datetime_for_filename(dt: datetime) -> str:
    if dt == None: return None
    return dt.strftime('%Y%m%d-%H%M%S')

eastern_time_zone = pytz.timezone("US/Eastern")

def format_datetime_for_log(dt: datetime) -> str:
    if dt == None: return "[none]"
    if type(dt) == str:
        logger.warning("input date is a string, try parsing with isoformat")
        dt = datetime.fromisoformat(dt)
    return dt.astimezone(eastern_time_zone).strftime('%Y-%m-%d %H:%M:%S %Z')

def format_datetime_for_display(dt: datetime) -> str:
    if dt == None: return ""
    if type(dt) == str:
        logger.warning("input date is a string, try parsing with isoformat")
        dt = datetime.fromisoformat(dt)
    return dt.astimezone(eastern_time_zone).strftime('%Y-%m-%d %H:%M:%S %Z')

def format_datetime_difference(t1: datetime, t2: datetime) -> str:
    if t1 == None or t2 == None: return ""
    if type(t1) == str:
        logger.warning("input date is a string, try parsing with isoformat")
        t1 = datetime.fromisoformat(t1)
    if type(t2) == str:
        logger.warning("input date is a string, try parsing with isoformat")
        t2 = datetime.fromisoformat(t2)


    if t1 < t2: return "NOW"
    delta = t1 - t2
    if delta.days != 0:
        return "OLD"
    else:
        sec = delta.seconds
        h = sec // (60*60)
        m = (sec - h * 60*60) // 60
        return f"{h:02d}:{m:02d}"

def is_isoformated_str(s: str) -> False:
    if type(s) != str: return False
    #2020-03-13T06:17:50.204477
    return re.match("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{6})?", s)

def convert_json_to_python(x):
    if x is None:
        pass
    elif type(x) == str:
        if is_isoformated_str(x):
            x = datetime.fromisoformat(x)
    elif type(x) == float:
        pass
    elif type(x) == int:
        pass
    elif type(x) == bool:
        pass
    elif type(x) == dict:
        for n in x:
            v = x[n]
            x[n] = convert_json_to_python(v)
    elif type(x) == list:
        for i in len(x):
            v = x[i]
            x[i] = convert_json_to_python(v)
    else:
        raise Exception(f"unexpected type: {type(x)}")
    return x 

def convert_python_to_json(x):
    if x is None:
        pass
    elif type(x) == str:
        if is_isoformated_str(x):
            raise Exception("invalid str, content would be converted to datetime on load")
    elif type(x) == datetime:
        x = x.toisformat()
    elif type(x) == float:
        pass
    elif type(x) == int:
        pass
    elif type(x) == bool:
        pass
    elif type(x) == dict:
        for n in x:
            v = x[n]
            x[n] = convert_python_to_json(v)
    elif type(x) == list:
        for i in len(x):
            v = x[i]
            x[i] = convert_python_to_json(v)
    else:
        raise Exception(f"unexpected type: {type(x)}")
    return x 



# -----

def get_host():
    host = os.environ.get("HOST")
    if host == None: host = os.environ.get("COMPUTERNAME")
    return host

# -----

def is_python_code_dir(xdir: str) -> bool:
    python_test_dir = os.path.join(xdir, "__pycache__")
    if os.path.exists(python_test_dir): return True
    return False


def git_push(git_dir: str, commit_msg: str):
    """ run git add/commit/push """

    logger.info("=====================================================================")
    logger.info(f"==== | run: pushd {git_dir} && git commit -a -m \"{commit_msg}\" && git push && popd")
    logger.info("=====================================================================")

    wd = os.getcwd()
    os.chdir(git_dir)
    try:
        if not os.path.exists(".git"): raise Exception("Missing .git directory")

        
        logger.info(f"git commit -a -m {commit_msg}")
        subprocess.call(["git", "commit", "-a", "-m", commit_msg])
        logger.info("git push")
        subprocess.call(["git", "push"])

        logger.info("==== done")
    finally:
        os.chdir(wd)


def git_isbehind(git_dir: str = ".") -> bool:
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

    
def git_pull(git_dir: str = "."):
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
            if git_isbehind(): 
                logger.info("pull new sources...")
                git_pull()
                return True 
    return False
