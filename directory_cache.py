import os
import shutil
import re
import datetime

from datetime import datetime, timezone
from loguru import logger
import pytz

from typing import Union, List, Tuple, Dict

from util import file_age, format_mins

class DirectoryCache:
    """  a simple disk-based page cache """

    def __init__(self, work_dir: str):
        self.work_dir = work_dir

        if not os.path.isdir(self.work_dir):
            os.makedirs(self.work_dir)

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

        dt = datetime.now(timezone.utc).astimezone()
        new_date = f"{dt} ({dt.tzname()})" 
        with open(xpath, "w") as f:
            f.write(f"{new_date}\n")
        return new_date, old_date
    
    def get_cache_age(self, key: str) -> float:
        file_name = self.encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)
        if not os.path.isfile(xpath): return 10000

        xdelta = file_age(xpath)
        return xdelta

    def read_date_time_str(self, key: str) -> float:
        file_name = self.encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)
        if not os.path.isfile(xpath): return "Missing"

        mtime = os.path.getmtime(xpath)
        mtime = datetime.fromtimestamp(mtime)
        dt = mtime

        xdelta = file_age(xpath)
        return f"changed at {dt} ({dt.tzname()}): {format_mins(xdelta)} ago" 


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


    def load(self, key: str) -> Union[bytes, None]:

        file_name = self.encode_key(key)

        xpath = os.path.join(self.work_dir, file_name)
        if not os.path.isfile(xpath): return None

        r = open(xpath, "rb")
        try:
            content = r.read()
            return content
        finally:
            r.close()

    def import_file(self, key: str, src) -> str:

        xkey = self.encode_key(key)
        xto_path = os.path.join(self.work_dir, xkey)
        
        if type(src) is str:
            xfrom_path = src
        elif type(src) == type(self):
            xfrom_path = os.path.join(src.work_dir, key)
        else:
            raise Exception("Destination must be str or DirectoryCache")

        if os.path.exists(xto_path): 
            if os.path.samefile(xfrom_path, xto_path): return
            os.remove(xto_path)
        if os.path.exists(xfrom_path): shutil.copy(xfrom_path, xto_path)
        return xkey

    def export_file(self, key: str, dest, new_key: str= None):

        xfrom_path = os.path.join(self.work_dir, self.encode_key(key))
        
        if type(dest) is str:
            xto_path = os.path.join(dest, new_key) if new_key != None else dest
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


    def save(self, content: bytes, key: str):

        if content == None: return
        if not isinstance(content, bytes):
            raise TypeError("content must be type 'bytes'")

        file_name = self.encode_key(key)
        xpath = os.path.join(self.work_dir, file_name)

        with open(xpath, "wb") as w:
            w.write(content)

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
