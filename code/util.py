import os
import sys
import requests
from loguru import logger
import re
from typing import Tuple, List, Dict, Callable
import sys
from datetime import datetime

from requests.packages import urllib3
import udatetime

urllib3.disable_warnings() 

def fetch_with_requests(page: str) -> [bytes, int]:
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


def convert_json_to_python(x):
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
    host = os.environ.get("HOST")
    if host == None: host = os.environ.get("COMPUTERNAME")
    return host

# -----

