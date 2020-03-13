import os
import shutil
import re
import datetime
import requests

from datetime import datetime, timezone
from loguru import logger
import pytz

from typing import Union, List, Tuple, Dict

from requests.packages import urllib3
urllib3.disable_warnings() 

def encode_key(key: str) -> str:
    """ convert to a file-stystem safe representation of a URL """

    result = re.sub("https?:.+/", "", key)
    result = re.sub("[/?=&]", "_", result)
    result = result.replace(".aspx", ".html")
    return result

def file_age(xpath: str) -> float:
    """ get age of a file in minutes """

    #print(xpath)
    mtime = os.path.getmtime(xpath)
    mtime = datetime.fromtimestamp(mtime)

    xnow = datetime.now()
    xdelta = (xnow - mtime).seconds / 60.0

    # print(f" time = {mtime}")
    # print(f" now = {xnow}")
    # print(f" delta = {xdelta}")

    return xdelta


class PageCache:
    """  a simple disk-based page cache """

    def __init__(self, work_dir: str):
        self.work_dir = work_dir

        if not os.path.isdir(self.work_dir):
            os.makedirs(self.work_dir)

    def read_old_date(self) -> str:
        xpath = os.path.join(self.work_dir, "time_stamp.txt")
        if os.path.exists(xpath):
            with open(xpath, "r") as f:
                old_date = f.readline()
        else:
            old_date = "[NONE]"
        return old_date

    def update_dates(self) -> Tuple[str, str]:
        " returns new and old date "
        xpath = os.path.join(self.work_dir, "time_stamp.txt")
        if os.path.exists(xpath):
            with open(xpath, "r") as f:
                old_date = f.readline()
        else:
            old_date = "[NONE]"

        dt = datetime.now(timezone.utc).astimezone()
        new_date = f"{dt} ({dt.tzname()})" 
        with open(xpath, "w") as f:
            f.write(f"{new_date}\n")
        return new_date, old_date

    
    def fetch(self, page: str) -> [bytes, int]:
        #print(f"fetch {page}")
        try:
            resp = requests.get(page, verify=False)
            return resp.content, resp.status_code
        except Exception as ex:
            logger.error(f"Exception: {ex}")
            return None, 999

    def get_cache_age(self, key: str) -> float:
        file_name = encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)
        if not os.path.isfile(xpath): return 10000

        xdelta = file_age(xpath)
        return xdelta

    def read_date_time_str(self, key: str) -> float:
        file_name = encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)
        if not os.path.isfile(xpath): return "Missing"

        mtime = os.path.getmtime(xpath)
        mtime = datetime.fromtimestamp(mtime)
        dt = mtime

        def format_mins(x : float):
            if x < 60.0:
                return f"{x:.0f} mins"
            x /= 60.0
            if x < 24.0:
                return f"{x:.1f} hours"
            return f"{x:.1f} days"

        xdelta = file_age(xpath)
        return f"changed at {dt} ({dt.tzname()}): {format_mins(xdelta)} ago" 


    def does_version_exists(self, key: str, version: str) -> bool:
        file_name = encode_key(key)
        if version == None:
            xpath = os.path.join(self.work_dir, file_name)
        else:
            xpath = os.path.join(self.work_dir, version, file_name)
        return os.path.exists(xpath)

    def list_html_files(self) -> List[str]:

        result = []
        for x in os.listdir(self.work_dir):
            if not x.endswith(".html"): continue
            result.append(x)
        return result


    def load(self, key: str, version: str) -> Union[bytes, None]:

        file_name = encode_key(key)

        if version != None:
            xdir = os.path.join(self.work_dir, version)
            if not os.path.exists(xdir): os.makedirs(xdir)            
        else:
            xdir = self.work_dir

        xpath = os.path.join(xdir, file_name)
        if not os.path.isfile(xpath): return None

        r = open(xpath, "rb")
        try:
            content = r.read()
            return content
        finally:
            r.close()

    def copy_to_version(self, key: str, version: str):

        if version == None:
            raise Exception("Missing version")

        xdir = os.path.join(self.work_dir, version)
        if not os.path.exists(xdir): os.makedirs(xdir)            

        xfrom = os.path.join(self.work_dir, key)
        xto = os.path.join(self.work_dir, version, key)

        if os.path.exists(xto): os.remove(xto)
        if os.path.exists(xfrom): shutil.copy(xfrom, xto)
            


    def save(self, content: bytes, key: str, version: str):

        if content == None: return
        if not isinstance(content, bytes):
            raise TypeError("content must be type 'bytes'")

        if version != None:
            xdir = os.path.join(self.work_dir, version)
            if not os.path.exists(xdir): os.makedirs(xdir)            
        else:
            xdir = self.work_dir

        file_name = encode_key(key)
        xpath = os.path.join(xdir, file_name)

        w = open(xpath, "wb")
        try:
            w.write(content)
        finally:
            w.close()

    def cleanup(self, max_age_mins: int):
        for fn in os.listdir(self.work_dir):
            xpath = os.path.join(self.work_dir, fn)
            if file_age(xpath) > max_age_mins:
                os.remove(xpath)

    def reset(self):
        if not os.path.isdir(self.work_dir): return

        for fn in os.listdir(self.work_dir):
            xpath = os.path.join(self.work_dir, fn)
            os.remove(xpath)
