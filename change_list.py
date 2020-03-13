import os
import json
from loguru import logger
from typing import Dict, Tuple
from datetime import datetime

from directory_cache import DirectoryCache

class ChangeList:
    """ maintains a list of changes for a run """

    __slots__ = (
        'cache',

        'start_date',
        'end_date',
        'previous_date',
        'time_lapsed',
        'error_message',
        'complete',

        '_items',
        '_lookup',
    )

    def __init__(self, cache: DirectoryCache):
        self.cache = cache

        self.start_date = datetime.utcnow()
        self.end_date = self.start_date
        self.previous_date = self.start_date
        self.time_lapsed = self.end_date - self.start_date
        self.error_message = None
        self.complete = False

        self._items = []
        self._lookup = {}
        
    def start_run(self):
        self._read_json()

        self.previous_date = self.start_date
        self.start_date = datetime.utcnow()
        self.end_date = self.start_date
        self.time_lapsed = self.end_date - self.start_date
        
        self.error_message = None
        self.complete = False
        for x in self._items: x["complete"] = False


    def save_progress(self):
        self.end_date = datetime.utcnow()
        self.time_lapsed = self.end_date - self.start_date

        self._write_json()
        self._write_text()
        self._write_urls()

    def abort_run(self, ex: Exception):
        self.error_message = str(ex)

    def finish_run(self):
        self.complete = True
        self.save_progress()

    def get_minutes_since_last_check(self, name: str) -> float:
        " get time since last check in minutes "
        
        idx = self._lookup.get(name)
        if idx == None: return 100000.0

        x = self._items[idx]
        checked_date = x["checked"]
        if checked_date == None: return 100000.0
        delta = self.start_date - datetime.fromisoformat(checked_date)

        return delta.total_seconds() / 60.0

    def update_status(self, name: str, status: str, xurl: str, msg: str) -> Tuple[Dict, Dict, str]:
        
        xnow = datetime.utcnow().isoformat()

        if msg == "": msg = None
        idx = self._lookup.get(name)
        if idx == None:         
            x = None   
            y = { "name": name, "status": status, "url": xurl, "msg": msg, "complete": True,
                  "added": xnow, "checked": None, "updated": None, "failed": None }
            self._lookup[name] = len(self._items)
            self._items.append(y)
        else:
            x = self._items[idx]
            y = x.copy()
            y["status"] = status
            y["url"] = xurl
            y["msg"] = msg
            y["complete"] = True
            self._items[idx] = y
            
        return y, x, xnow

    def record_failed(self, name: str, xurl: str, msg: str):

        status = "FAILED"
        logger.error(f"     {name}: {status}")
        logger.error(f"     {name}: url={xurl} msg={msg}")

        y, x, xnow = self.update_status(name, status, xurl, msg)
        if x != None and x["status"] != "FAILED":
            y["failed"] = xnow


    def record_unchanged(self, name: str, xurl: str, msg: str = ""):

        status = "unchanged"
        logger.info(f"  {name}: {status}")

        y, _, xnow = self.update_status(name, status, xurl, msg)
        y["checked"] = xnow
        y["failed"] = None

    def record_skip(self, name: str, xurl: str, msg: str = ""):

        status = "skip"
        logger.info(f"  {name}: {status} {msg}")

        self.update_status(name, status, xurl, msg)

    def temporary_skip(self, name: str, xurl: str, msg: str = ""):
        " make as complete but don't change state "
        status = "skip"
        logger.info(f"  {name}: {status} {msg}")

        y, x, _ = self.update_status(name, status, xurl, msg)
        if x != None:
            y["status"] = x["status"]
            y["msg"] = x["msg"]


    def record_changed(self, name: str, xurl: str, msg: str = ""):

        status = "CHANGED"
        logger.warning(f"    {name}: {status}")

        y, _, xnow = self.update_status(name, status, xurl, msg)
        y["checked"] = xnow
        y["updated"] = xnow
        y["failed"] = None


    def _remove_text_files(self):
        for n in ["change_list.txt", "urls.txt"]:
            fn = os.path.join(self.cache.work_dir, n)
            if os.path.exists(fn): os.remove(fn)

    def _write_text(self):
        fn = os.path.join(self.cache.work_dir, "change_list.txt")
        
        changed = []
        unchanged = []
        skipped = []
        for x in self._items:
            name = x["name"]
            status = x["status"]

            if status == "skipped":
                skipped.append(x)
            elif status == "unchanged":
                unchanged.append(x)
            else:
                changed.append(x)

        with open(fn, "w") as f_changes:
            f_changes.write(f"STATE CHANGE LIST\n\n")
            f_changes.write(f"  start\t{self.start_date}\n")
            f_changes.write(f"  end\t{self.end_date}\n")
            f_changes.write(f"  previous\t{self.previous_date}\n")
            f_changes.write(f"  lapsed\t{self.time_lapsed}\n")
            f_changes.write(f"\n")

            f_changes.write(f"  changed\t{len(changed)}\n")
            f_changes.write(f"  unchanged\t{len(unchanged)}\n")
            f_changes.write(f"  skipped\t{len(skipped)}\n")
            f_changes.write(f"\n")

            f_changes.write("====== ITEMS THAT HAVE CHANGED ======\n")
            for x in changed:
                name, status, xurl, msg = x["name"], x["status"], x["url"], x["msg"]
                if msg == None: msg = ""            
                f_changes.write(f"{name}\t{status}\t{xurl}\t{msg}\n")
            f_changes.write(f"\n\n")

            f_changes.write("====== ITEMS HAVE NOT CHANGED ======\n")
            for x in unchanged:
                name, status, xurl, msg = x["name"], x["status"], x["url"], x["msg"]
                if msg == None: msg = ""            
                f_changes.write(f"{name}\t{status}\t{xurl}\t{msg}\n")

            f_changes.write("====== ITEMS WHERE SKIPPED ======\n")
            for x in skipped:
                name, status, xurl, msg = x["name"], x["status"], x["url"], x["msg"]
                if msg == None: msg = ""            
                f_changes.write(f"{name}\t{status}\t{xurl}\t{msg}\n")

    def _write_json(self):
        logger.info(f"time lapsed {self.time_lapsed}")

        fn = os.path.join(self.cache.work_dir, "change_list.json")

        result = {}        
        result["start_date"] = self.start_date.isoformat() 
        result["end_date"] = self.end_date.isoformat()
        result["previous_date"] = self.previous_date.isoformat()
        result["time_lapsed"] = str(self.time_lapsed)
        result["complete"] = self.complete 
        result["error_message"] = self.error_message 

        result["items"] = self._items

        with open(fn, "w") as f_changes:
            json.dump(result, f_changes, indent=2)

    def _read_json(self):
        fn = os.path.join(self.cache.work_dir, "change_list.json")

        self._items = []
        if not os.path.exists(fn): return

        with open(fn, "r") as f_changes:
            result = json.load(f_changes)

        self.start_date = datetime.fromisoformat(result["start_date"])  
        self.end_date = datetime.fromisoformat(result["end_date"])
        self.previous_date = datetime.fromisoformat(result["previous_date"]) 
        self.time_lapsed = self.end_date - self.start_date
        self.error_message = result["error_message"] 

        self._items = result["items"]

        self._lookup = { }
        for idx in range(len(self._items)): 
            n = self._items[idx]["name"]
            self._lookup[n] = idx



    def _write_urls(self):
        fn = os.path.join(self.cache.work_dir, "urls.txt")
        with open(fn, "w") as furl:
            furl.write("Name\tUrl\n")
            for x in self._items:
                name, xurl = x["name"], x["url"]
                furl.write(f"{name}\t{xurl}\n")

    def read_urls_as_dict(self) -> Dict:
        result = {}
        with open(os.path.join(self.cache.work_dir, "urls.txt")) as f:
            lines = f.readlines()
            for x in lines[1:]:
                name, xurl = x.split("\t")
                name = name.replace(".html", "")
                result[name] = xurl[:-1]
        return result

