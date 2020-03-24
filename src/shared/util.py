import os
import sys
import requests
from loguru import logger
import re
from typing import Tuple, List, Dict, Callable
from datetime import datetime
from requests.packages import urllib3
import configparser

from shared import udatetime

urllib3.disable_warnings() 

def fetch_with_requests(page: str) -> [bytes, int]:
    " check data using requests "
    try:
        resp = requests.get(page, verify=False, timeout=30)
        return resp.content, resp.status_code
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return None, 999

def is_bad_content(content: bytes) -> [bool, str]:
    " checks if content returned from requests looks bad "

    if content == None: return True, "Empty Response"
    if len(content) < 600: return True, f"Response is {len(content)} bytes"
    if re.search(b"Request unsuccessful. Incapsula incident", content):
        return True, f"Site uses Incapsula"
    return False, None


def convert_json_to_python(x):
    """ convert a data collection from json compatible format 
    
    1. operates on data inplace
    2. input should be a list or a dict
    3. deals with parsing strings into dates.

    Assumes all input dates that match the ISO standard are
    UTC datetimes stored as strings in json and parses.

    normal usage:
        x = json.loads(s)
        convert_json_to_python(x)

    """
    if x is None:
        pass
    elif type(x) == str:
        if udatetime.is_isoformated(x):
            x = udatetime.from_json(x)
    elif type(x) == datetime:
        raise Exception("JSON doesn't parse dates")
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
        for i in range(len(x)):
            v = x[i]
            x[i] = convert_json_to_python(v)
    else:
        raise Exception(f"unexpected type: {type(x)}")
    return x 

def convert_python_to_json(x):
    """ convert a data collection to a json compatible format 
    
    1. operates on data inplace
    2. input should be a list or a dict
    3. formats datetime as ISO string

    Assumes all input datetimes are tz-aware UTC and formats
    them as ISO strings.  Fails if you hand it a date as an
    ISO string because it would be ambiguous when reloaded.

    normal usage:
        convert_python_to_json(x)
        s = json.dumps(x)

    """
    if x is None:
        pass
    elif type(x) == str:
        if udatetime.is_isoformated(x):
            raise Exception("ambiguous str, content would be converted to datetime on load")
    elif type(x) == datetime:
        x = x.isoformat()
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
        for i in range(len(x)):
            v = x[i]
            x[i] = convert_python_to_json(v)
    else:
        raise Exception(f"unexpected type: {type(x)}")
    return x 

# -----

def get_host():
    " get the name of the hosting computer "
    host = os.environ.get("HOST")
    if host == None: host = os.environ.get("COMPUTERNAME")
    return host

# -----

def read_config_file() -> configparser.ConfigParser:
    " read the ini file in the base repo dir"

    ini_dir = os.path.join(os.path.dirname(__file__), "..", "..")
    ini_dir = os.path.abspath(ini_dir)

    config = configparser.ConfigParser()
    for fn in ["data_pipeline.local.ini", "data_pipeline.ini"]:
        p = os.path.join(ini_dir, fn)
        if os.path.exists(p):
            config.read(p)
            return config

    raise Exception(f"Missing data_pipeline.ini file in {ini_dir}")

def find_executable(name: str) -> str:
    " find an executable in current or parent directory or PATH"

    path = os.environ.get("PATH")
    dirs = path.split(";")

    dirs.insert(0, ".")
    dirs.insert(0, "..")
    for d in dirs:
        p = os.path.join(d, name)
        if os.path.isfile(p): return p
    return None
