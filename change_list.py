import os
import json
from loguru import logger
from typing import Dict

from directory_cache import DirectoryCache

class ChangeList:
    """ maintains a list of changes for a run """

    def __init__(self, cache: DirectoryCache):
        self.cache = cache
        new_date, old_date = self.cache.update_dates()
        self.new_date = new_date
        self.old_date = old_date

        self.items = []

    def record_failed(self, name: str, xurl: str, msg: str):

        status = "FAILED"
        logger.error(f"     {name}: {status}")
        logger.error(f"     {name}: url={xurl} msg={msg}")

        x = { "name": name, "status": status, "url": xurl, "msg": msg}
        self.items.append(x)

    def record_unchanged(self, name: str, xurl: str, msg: str = ""):

        status = "unchanged"
        logger.info(f"  {name}: {status}")

        x = { "name": name, "status": status, "url": xurl, "msg": msg}
        self.items.append(x)

    def record_changed(self, name: str, xurl: str, msg: str = ""):

        status = "CHANGED"
        logger.warning(f"    {name}: {status}")

        x = { "name": name, "status": status, "url": xurl, "msg": msg}
        self.items.append(x)

    def record_needs_check(self, name: str, xurl: str, msg: str = ""):

        status = "CHECK"
        logger.warning(f"{name}: {status}")

        x = { "name": name, "status": status, "url": xurl, "msg": msg}
        self.items.append(x)

    def remove_output_files(self):
        for n in ["change_list.txt", "change_list.json", "urls.txt"]:
            fn = os.path.join(self.cache.work_dir, n)
            if os.path.exists(fn): os.remove(fn)

    def write_text(self):
        fn = os.path.join(self.cache.work_dir, "change_list.txt")
        
        flagged = []
        unchanged = []
        for x in self.items:
            name = x["name"]
            status = x["status"]

            if status == "unchanged":
                unchanged.append(x)
            else:
                flagged.append(x)

        with open(fn, "w") as f_changes:
            f_changes.write(f"STATE CHANGE LIST\n\n")
            f_changes.write(f"  current run\t{self.new_date}\n")
            f_changes.write(f"  previous run\t{self.old_date}\n")
            f_changes.write(f"\n")

            f_changes.write(f"  flagged items\t{len(flagged)}\n")
            f_changes.write(f"  unchanged items\t{len(unchanged)}\n")
            f_changes.write(f"\n")

            f_changes.write("====== ITEMS THAT NEED ATTENTION ======\n")
            for x in flagged:
                name, status, xurl, msg = x["name"], x["status"], x["url"], x["msg"]            
                f_changes.write(f"{name}\t{status}\t{xurl}\t{msg}\n")
            f_changes.write(f"\n\n")

            f_changes.write("====== ITEMS HAVE NOT CHANGED ======\n")
            for x in unchanged:
                name, status, xurl, msg = x["name"], x["status"], x["url"], x["msg"]
                f_changes.write(f"{name}\t{status}\t{xurl}\t{msg}\n")

    def write_json(self):
        fn = os.path.join(self.cache.work_dir, "change_list.json")

        result = {}
        result["current_run"] = self.new_date
        result["previous_run"] = self.old_date
        result["items"] = self.items

        with open(fn, "w") as f_changes:
            json.dump(result, f_changes, indent=2)

    def read_json(self):
        fn = os.path.join(self.cache.work_dir, "change_list.json")

        with open(fn, "w") as f_changes:
            result = json.load(f_changes)

        self.new_date = result["current_run"]
        self.old_date = result["previous_run"]
        self.items = result["items"]

    def write_urls(self):
        fn = os.path.join(self.cache.work_dir, "urls.txt")
        with open(fn, "w") as furl:
            furl.write("Name\tUrl\n")
            for x in self.items:
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

