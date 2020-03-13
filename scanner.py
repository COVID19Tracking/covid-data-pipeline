import os
import re
from datetime import datetime, timezone
import pytz
import subprocess
from loguru import logger
from typing import List, Dict, Tuple
import urllib.parse

from directory_cache import DirectoryCache
from change_list import ChangeList
from html_compare import HTMLCompare
from sheet_parser import SheetParser

from cov19_regularize import regularize

from util import fetch, is_bad_content

class PageScanner():

    def __init__(self, base_dir:str, main_sheet_url: str):
        self.main_sheet_url = main_sheet_url
        
        self.base_dir = base_dir
        self.cache_raw = DirectoryCache(os.path.join(base_dir, "raw")) 
        self.cache_cleaned = DirectoryCache(os.path.join(base_dir, "cleaned")) 
        self.cache_diff = DirectoryCache(os.path.join(base_dir, "diff")) 

        self.trace = False

    def fetch_from_sources(self) -> Dict[str, str]:
        " load the google sheet and parse out the individual state URLs"

        host = os.environ.get("HOST")
        if host == None: host = os.environ.get("COMPUTERNAME")

        change_list = ChangeList(self.cache_raw)        
        
        print(f"run started on {host} at {change_list.start_date.isoformat()}")
        
        change_list.start_run()
        try:
            return self._main_loop(change_list)
        except Exception as ex:
            logger.exception(ex)
            change_list.abort_run(ex)
        finally:
            change_list.finish_run()

            print(f"run finished on {host} at {change_list.start_date.isoformat()}")
            self.save_to_github(f"{change_list.start_date.isoformat()} on {host}")


    def save_to_github(self, commit_msg: str):
        logger.info("commiting changes...")
        subprocess.call(["git", "commit", "-a", "-m", commit_msg])
        logger.info("pushing changes...")
        subprocess.call(["git", "push"])
        logger.info("done")


    def _main_loop(self, change_list: ChangeList) -> Dict[str, str]:

        def clean_url(s: str) -> str:
            if s == None or s == "": return None
            idx = s.find("?q=")
            if idx < 0: return s
            idx += 3
            eidx = s.find("&", idx)
            if eidx < 0: eidx = len(s) 
            s = s[idx:eidx]
            s =  urllib.parse.unquote_plus(s)
            return s

        def fetch_if_changed(state: str, xurl: str, skip: bool = False) -> bool:

            key = state + ".html"

            if xurl == "" or xurl == None or xurl == "None": 
                change_list.record_skip(key, xurl, "missing url")
                return

            mins = change_list.get_minutes_since_last_check(key)
            if self.trace: logger.info(f"  checked {key} {mins:.1f} minutes ago")
            if mins < 15: 
                logger.info(f"{key}: skip b/c checked < 15 mins")
                change_list.temporary_skip(key, xurl, "age < 15 mins")
                return False

            if skip:
                change_list.record_skip(key, xurl, "skip flag set")
                return False

            local_content =  self.cache_raw.load(key)

            if self.trace: logger.info(f"fetch {xurl}")
            remote_content, status = fetch(xurl)
            is_bad, msg = is_bad_content(remote_content)
            if is_bad:
                change_list.record_failed(key, xurl, msg)
                return False

            if status > 300:
                change_list.record_failed(state, xurl, f"HTTP status {status}")
                return False

            remote_content = regularize(remote_content)

            if local_content != remote_content:
                self.cache_raw.save(remote_content, key)
                change_list.record_changed(key, xurl)
            else:
                change_list.record_unchanged(key, xurl)
                return False

        # -- get google worksheet

        logger.info("fetch main page...")
        
        fetch_if_changed("main_sheet.html", self.main_sheet_url)
        content = self.cache_raw.load("main_sheet.html") 

        parser = SheetParser()
        df_config = parser.get_config(content)


        # -- fetch pages found in worksheet
        skip = False

        for idx, r in df_config.iterrows():
            state = r["State"]
            general_url = clean_url(r["COVID-19 site"])
            data_url = clean_url(r["Data site"])

            if general_url == None:
                logger.warning(f"  no main url for {state}")
                continue

            if idx % 10 == 1: change_list.save_progress()

            fetch_if_changed(state, general_url, skip=skip)
            if data_url != None:
                fetch_if_changed(state + "_data", data_url, skip=skip)



# --------------------
def main():

    base_dir = "C:\\data\\corona19-data-archive"
    main_sheet = "https://docs.google.com/spreadsheets/d/18oVRrHj3c183mHmq3m89_163yuYltLNlOmPerQ18E8w/htmlview?sle=true#"
    scanner = PageScanner(base_dir, main_sheet)
    scanner.fetch_from_sources()

if __name__ == "__main__":
    main()
