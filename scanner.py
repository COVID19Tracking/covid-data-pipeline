import os
import sys
import re
from datetime import datetime, timezone
import pytz
from loguru import logger
from typing import List, Dict, Tuple
import urllib.parse

from directory_cache import DirectoryCache
from change_list import ChangeList
from html_compare import HTMLCompare
from sheet_parser import SheetParser
from url_manager import UrlManager

#from cov19_regularize import regularize
from html_cleaner import HtmlCleaner

from util import is_bad_content, get_host, save_data_to_github

class ScannerOptions():

    def __init__(self):

        self.clean = False
        self.auto_push = False
        self.trace = False
        self.show_help = False

    def parse_args(self):
        for x in sys.argv[1:]:
            if x in ["-c", "--clean"]:
                self.clean = True
            elif x in ["--trace"]:
                self.trace = True
            elif x in ["-a", "--auto-push"]:
                self.auto_push = True
            elif x in ("-h", "--help"):
                self.show_help = True
            else:
                logger.error(f"unexpected option {x}")

    def get_help_text(self) -> str:
        return """
scanner: scan the COVID-19 government sites
    data is fetched and cleaned then pushed to a git repo
    files are only updated if the cleaned version changes

    -c, --clean:  run the cleaner on everything
    -a, --auto-push: checkin to the git repo at end of run
    --trace:  turn on tracing
"""

# -----

class PageScanner():

    def __init__(self, base_dir:str, main_sheet_url: str, options: ScannerOptions = None):
        self.main_sheet_url = main_sheet_url
        
        self.base_dir = base_dir
        self.cache_raw = DirectoryCache(os.path.join(base_dir, "raw")) 
        self.cache_clean = DirectoryCache(os.path.join(base_dir, "clean")) 
        self.cache_diff = DirectoryCache(os.path.join(base_dir, "diff")) 

        self.url_manager = UrlManager()

        self.html_cleaner = HtmlCleaner()

        if options == None: options = ScannerOptions()
        self.options = options

    def fetch_from_sources(self) -> Dict[str, str]:
        " load the google sheet and parse out the individual state URLs"

        change_list = ChangeList(self.cache_raw)        
        
        host = get_host()
        print(f"run started on {host} at {change_list.start_date.isoformat()}")
        
        change_list.start_run()
        try:
            return self._main_loop(change_list)
        except Exception as ex:
            logger.exception(ex)
            change_list.abort_run(ex)
        finally:
            change_list.finish_run()

            logger.info(f"  [in-memory content cache took {self.url_manager.size}")
            logger.info(f"run finished on {host} at {change_list.start_date.isoformat()}")
            
            if self.options.auto_push:
                save_data_to_github(self.base_dir, f"{change_list.start_date.isoformat()} on {host}")
            else:
                logger.warning("github push is DISABLED")

    def clean_html(self):
        # -- rebuild clean files (if necessary)
        is_first = False
        for key in self.cache_raw.list_html_files():
            if self.options.clean or not self.cache_clean.exists(key):
                if is_first:
                    logger.info(f"clean existing files...")
                    is_first = False
                logger.info(f"  clean {key}")
                local_raw_content =  self.cache_raw.load(key)
                local_clean_content = self.html_cleaner.Clean(local_raw_content)
                self.cache_clean.save(local_clean_content, key)


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
            if self.options.trace: logger.info(f"  checked {key} {mins:.1f} minutes ago")
            if mins < 15: 
                logger.info(f"{key}: skip b/c checked < 15 mins")
                change_list.temporary_skip(key, xurl, "age < 15 mins")
                return False

            if skip:
                change_list.record_skip(key, xurl, "skip flag set")
                return False

            if self.options.trace: logger.info(f"fetch {xurl}")
            remote_raw_content, status = self.url_manager.fetch(xurl)
            is_bad, msg = is_bad_content(remote_raw_content)
            if is_bad:
                change_list.record_failed(key, xurl, msg)
                return False

            if status > 300:
                change_list.record_failed(state, xurl, f"HTTP status {status}")
                return False

            local_clean_content =  self.cache_clean.load(key)
            remote_clean_content = self.html_cleaner.Clean(remote_raw_content)

            if local_clean_content != remote_clean_content:
                self.cache_raw.save(remote_raw_content, key)
                self.cache_clean.save(remote_clean_content, key)
                change_list.record_changed(key, xurl)
            else:
                change_list.record_unchanged(key, xurl)
                return False

        self.clean_html()

        # -- get google worksheet
        logger.info("fetch main page...")
        
        fetch_if_changed("main_sheet", self.main_sheet_url)
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



def main():

    options = ScannerOptions()    
    options.parse_args()

    if options.show_help:
        print(options.get_help_text())
        exit(-1)

    base_dir = "C:\\data\\corona19-data-archive"
    main_sheet = "https://docs.google.com/spreadsheets/d/18oVRrHj3c183mHmq3m89_163yuYltLNlOmPerQ18E8w/htmlview?sle=true#"
    scanner = PageScanner(base_dir, main_sheet, options=options)
    
    if options.clean:
        scanner.clean_html()
    else:
        scanner.fetch_from_sources()

if __name__ == "__main__":
    main()
