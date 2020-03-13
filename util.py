import os
import requests
from datetime import datetime, timezone
import pytz
from loguru import logger
import re
import subprocess

from requests.packages import urllib3
urllib3.disable_warnings() 

def fetch(page: str) -> [bytes, int]:
    #print(f"fetch {page}")
    try:
        resp = requests.get(page, verify=False)
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

def format_datetime_for_file(dt: datetime):
    #return dt.isoformat().replace(":", "_x_").replace("+", "_p_")
    return dt.strftime('%Y%m%d-%H%M%S')

def get_host():
    host = os.environ.get("HOST")
    if host == None: host = os.environ.get("COMPUTERNAME")
    return host

def is_code_dir(xdir: str) -> bool:

    python_test_dir = os.path.join(xdir, "__pycache__")
    if os.path.exists(python_test_dir): return True

    return False

def save_data_to_github(git_dir: str, commit_msg: str):

    if is_code_dir(git_dir):
        raise Exception(f"{git_dir} because it looks like a code directory")

    wd = os.getcwd()
    os.chdir(git_dir)
    try:
        logger.info("adding data changes...")
        subprocess.call(["git", "add", "."])
        logger.info("commiting data changes...")
        subprocess.call(["git", "commit", "-m", commit_msg])
        logger.info("pushing data changes...")
        subprocess.call(["git", "push"])
        logger.info("done")
    finally:
        os.chdir(wd)

