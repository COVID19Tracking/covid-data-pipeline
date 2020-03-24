import os
import shutil
import re
import datetime
import time

from datetime import datetime, timezone
from loguru import logger
import pytz

from typing import Union, List, Tuple, Dict
#import subprocess

from shared import udatetime

class DirectoryCache:
    """  a simple disk-based page cache """

    def __init__(self, work_dir: str, trace: bool=False):
        self.work_dir = work_dir

        if not os.path.isdir(self.work_dir):
            os.makedirs(self.work_dir)

        self.trace = trace

    def encode_key(self, key: str) -> str:
        """ convert to a file-stystem safe representation of a URL """

        result = re.sub("https?:.+/", "", key)
        result = re.sub("[/?=&]", "_", result)
        result = result.replace(".aspx", ".html")
        return result

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

        dt = udatetime.now_as_utc()
        new_date = f"{dt} ({dt.tzname()})" 
        with open(xpath, "w") as f:
            f.write(f"{new_date}\n")
        return new_date, old_date
    
    def get_cache_age(self, key: str) -> float:
        file_name = self.encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)
        if not os.path.isfile(xpath): return 10000

        xdelta = udatetime.file_age(xpath)
        return xdelta

    def read_date_time_str(self, key: str) -> float:
        file_name = self.encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)
        if not os.path.isfile(xpath): return "Missing"

        dt = udatetime.file_age(xpath)

        xdelta = udatetime.file_age(xpath)
        return f"changed at {udatetime.to_displayformat(dt)}): {udatetime.format_mins(xdelta)} ago" 


    def exists(self, key: str) -> bool:
        file_name = self.encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)
        return os.path.exists(xpath)

    def list_html_files(self) -> List[str]:

        result = []
        for x in os.listdir(self.work_dir):
            if not x.endswith(".html"): continue
            result.append(x)
        return result

    def list_files(self) -> List[str]:

        result = []
        for x in os.listdir(self.work_dir):
            result.append(x)
        return result


    def import_file(self, key: str, src) -> str:

        xkey = self.encode_key(key)
        xto_path = os.path.join(self.work_dir, xkey)
        
        if type(src) is str:
            xfrom_path = src
        elif type(src) == type(self):
            xfrom_path = os.path.join(src.work_dir, key)
        else:
            raise Exception("Destination must be str or DirectoryCache")

        if self.trace: logger.debug(f"import from {xfrom_path} to {xto_path}")
        if os.path.exists(xto_path): 
            if os.path.samefile(xfrom_path, xto_path): return
            os.remove(xto_path)
        if os.path.exists(xfrom_path): shutil.copy(xfrom_path, xto_path)
        return xkey

    def export_file(self, key: str, dest, new_key: str= None):

        xfrom_path = os.path.join(self.work_dir, self.encode_key(key))
        
        if type(dest) is str:
            xto_path = os.path.join(dest, new_key) if new_key != None else dest
            if self.trace: logger.debug(f"export from {xfrom_path} to {xto_path}")
            if os.path.exists(xto_path): 
                if os.path.samefile(xfrom_path, xto_path): return
                os.remove(xto_path)
            if os.path.exists(xfrom_path): shutil.copy(xfrom_path, xto_path)
        elif type(dest) == type(self):             
            new_key = key if new_key == None else key
            dest.import_file(key, xfrom_path)
        else:
            raise Exception("Destination must be str or DirectoryCache")
        return self.encode_key(new_key)

    def read(self, key: str) -> Union[bytes, None]:

        file_name = self.encode_key(key)

        xpath = os.path.join(self.work_dir, file_name)

        for i in range(3):
            if not os.path.isfile(xpath): return None
            try:
                if self.trace: logger.debug(f"read {xpath}")
                with open(xpath, "rb") as f:
                    content = f.read()
            except Exception as ex:
                time.sleep(0.1)
                chk = os.path.isfile(xpath)
                if self.trace: logger.debug(f"read {xpath} failed, isfile={chk}")
                if i < 2:
                    logger.debug(f"read {xpath} retry") 
                    break
                raise ex

        return content

    def write(self, key: str, content: bytes):

        if content == None: return
        if not isinstance(content, bytes):
            raise TypeError("content must be type 'bytes'")

        file_name = self.encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)

        if self.trace: logger.debug(f"write {xpath}")
        with open(xpath, "wb") as f:
            f.write(content)

    def remove(self, key: str):

        file_name = self.encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)

        if os.path.exists(xpath):
            if self.trace: logger.debug(f"remove {xpath}")
            os.remove(xpath)


    def cleanup(self, max_age_mins: int):
        if not os.path.isdir(self.work_dir): return
        if self.trace: logger.debug(f"   cleanup {self.work_dir}")

        for fn in os.listdir(self.work_dir):
            xpath = os.path.join(self.work_dir, fn)
            if udatetime.file_age(xpath) > max_age_mins:
                if self.trace: logger.debug(f"   remove {xpath}")
                os.remove(xpath)

    def reset(self):
        if not os.path.isdir(self.work_dir): return
        if self.trace: logger.debug(f"   rest {self.work_dir}")

        for fn in os.listdir(self.work_dir):
            xpath = os.path.join(self.work_dir, fn)
            if self.trace: logger.debug(f"   remove {xpath}")
            os.remove(xpath)
