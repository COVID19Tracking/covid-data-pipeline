"""
scanner

scan the COVID-19 government sites
data is fetched and cleaned then pushed to a git repo
files are only updated if the cleaned version changes
"""

from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter
import os
import sys
import re
import requests
import io
from datetime import datetime, timezone, timedelta
import pandas as pd
import pytz
import time
from loguru import logger
from typing import List, Dict, Tuple

from directory_cache import DirectoryCache
from change_list import ChangeList
from sheet_parser import SheetParser

from url_manager import UrlManager
from url_source import UrlSource, get_available_sources

from html_cleaner import HtmlCleaner
from html_extracter import HtmlExtracter

from specialized_capture import SpecializedCapture, special_cases

from util import is_bad_content, get_host, save_data_to_github


parser = ArgumentParser(
    description=__doc__,
    formatter_class=RawDescriptionHelpFormatter)

parser.add_argument(
    '-c', '--clean', dest='clean', action='store_true', default=False,
    help='run the cleaner on everything')
parser.add_argument(
    '-x', '--extract', dest='extract', action='store_true', default=False,
    help='run the extractor on everything')
parser.add_argument('--trace', dest='trace', action='store_true', default=False,
    help='turn on tracing')
parser.add_argument('-a', '--auto_push', dest='auto_push', action='store_true', default=False,
    help='checkin to the git repo at end of run')
parser.add_argument('--rerun_now', dest='rerun_now', action='store_true', default=False,
    help='include items that were fetched in the last 15 minutes')
parser.add_argument('--continuous', dest='continuous', action='store_true', default=False,
    help='Run at 0:05 and 0:35')
parser.add_argument('-i', '--image', dest='capture_image', action='store_true', default=False,
    help='capture image after each change')

# data dir args

parser.add_argument(
    '--base_dir',
    default='C:\\data\\corona19-data-archive',
    help='Local GitHub repo dir for corona19-data-archive')

parser.add_argument(
    '--temp_dir',
    default='"c:\\temp\\public-cache"',
    help='Local temp dir for snapshots')


class PageScanner():

    def __init__(self, base_dir: str, args: Namespace):
        
        self.base_dir = base_dir
        self.cache_raw = DirectoryCache(os.path.join(base_dir, "raw")) 
        self.cache_clean = DirectoryCache(os.path.join(base_dir, "clean")) 
        self.cache_extract = DirectoryCache(os.path.join(base_dir, "extract")) 
        self.cache_diff = DirectoryCache(os.path.join(base_dir, "diff")) 

        self.url_manager = UrlManager()

        self.html_cleaner = HtmlCleaner()
        self.html_extracter = HtmlExtracter()

        self.options = args
        self._capture: SpecializedCapture = None 

    def get_capture(self) -> SpecializedCapture:
        if self._capture == None:
            publish_dir = os.path.join(self.options.base_dir, 'captive-browser')
            self._capture = SpecializedCapture(
                self.options.temp_dir, publish_dir)
        return self._capture

    def shutdown_capture(self):
        if self._capture != None:
            self._capture.close()
            if self.options.auto_push:
                self._capture.publish()
        self._capture = None

    def process(self) -> Dict[str, str]:
        " run the pipeline "

        change_list = ChangeList(self.cache_raw)        
        
        host = get_host()
        print(f"=== run started on {host} at {change_list.start_date.isoformat()}")

        change_list.start_run()
        try:
            return self._main_loop(change_list)
        except Exception as ex:
            logger.exception(ex)
            change_list.abort_run(ex)
        finally:
            change_list.finish_run()

            self.shutdown_capture()

            logger.info(f"  [in-memory content cache took {self.url_manager.size*1e-6:.1f} MBs")
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
                local_raw_content =  self.cache_raw.read(key)
                local_clean_content = self.html_cleaner.clean(local_raw_content)
                self.cache_clean.write(key, local_clean_content)


    def extract_html(self):
        # -- rebuild extract files (if necessary)
        is_first = False
        for key in self.cache_clean.list_html_files():
            if self.options.extract or not self.cache_extract.exists(key):
                if is_first:
                    logger.info(f"extract existing files...")
                    is_first = False
                logger.info(f"  extract {key}")
                local_clean_content =  self.cache_clean.read(key)

                attributes = {
                    "title": key,
                    "source": f"http://covid19-api.exemplartech.com/source/{key}",
                    "raw": f"http://covid19-api.exemplartech.com/raw/{key}",
                    "clean": f"http://covid19-api.exemplartech.com/clean/{key}"
                }

                local_extract_content = self.html_extracter.extract(local_clean_content, attributes)
                self.cache_extract.write(key, local_extract_content)


    def _main_loop(self, change_list: ChangeList) -> Dict[str, str]:

        def remove_duplicate_if_exists(location: str, source: str, other_state: str):
            key = location + ".html"

            self.cache_raw.remove(key)
            self.cache_clean.remove(key)
            change_list.record_duplicate(key, source, f"duplicate of {other_state}")

            if self.options.capture_image:
                c = self.get_capture()
                c.remove(location)

        def fetch_if_changed(location: str, source: str, xurl: str, skip: bool = False) -> bool:

            key = location + ".html"

            if xurl == "" or xurl == None or xurl == "None": 
                change_list.record_skip(key, source, xurl, "missing url")
                return

            mins = change_list.get_minutes_since_last_check(key)
            if self.options.trace: logger.info(f"  checked {key} {mins:.1f} minutes ago")
            if mins < 15.0: 
                if self.options.rerun_now:
                    logger.info(f"{key}: checked {mins:.1f} mins ago")
                else:
                    logger.info(f"{key}: checked {mins:.1f} mins ago -> skip b/c < 15 mins")
                    change_list.temporary_skip(key, xurl, "age < 15 mins")
                    return False

            if skip:
                change_list.record_skip(key, source, xurl, "skip flag set")
                return False

            if self.options.trace: logger.info(f"fetch {xurl}")
            remote_raw_content, status = self.url_manager.fetch(xurl)
            is_bad, msg = is_bad_content(remote_raw_content)
            if is_bad:
                change_list.record_failed(key, source, xurl, msg)
                return False

            if status > 300:
                change_list.record_failed(location, source, xurl, f"HTTP status {status}")
                return False

            local_clean_content =  self.cache_clean.read(key)
            remote_clean_content = self.html_cleaner.clean(remote_raw_content)

            if local_clean_content != remote_clean_content:
                self.cache_raw.write(key, remote_raw_content)
                self.cache_clean.write(key, remote_clean_content)
                change_list.record_changed(key, source, xurl)

                if self.options.capture_image:
                    c = self.get_capture()
                    c.screenshot(key, f"Screenshot for {location}", xurl)
            else:
                change_list.record_unchanged(key, source, xurl)
                return False

        self.clean_html()

        # -- get states info from API
        url_sources = get_available_sources()
        logger.info(f"processing source {url_sources[0].name}")
        df_config = url_sources[0].load()
        url_sources[0].save_if_changed(self.cache_raw, change_list)

        # -- fetch pages
        skip = False

        for idx, r in df_config.iterrows():
            location = r["location"]
            source = r["source_name"]
            general_url = r["main_page"]
            data_url = r["data_page"]

            if general_url == None and data_url == None:
                logger.warning(f"  no urls for {location} -> skip")
                change_list.record_skip(location)
                continue

            if idx % 10 == 1: change_list.save_progress()

            if general_url != None:
                fetch_if_changed(location, source, general_url, skip=skip)
            if data_url != None:
                if general_url == data_url:
                    remove_duplicate_if_exists(location + "_data", source, location)
                else:
                    fetch_if_changed(location + "_data", source, data_url, skip=skip)
            
# ---- 
def run_continuous(scanner: PageScanner):

    # temp code to get it running
    capture: SpecializedCapture = None
    if True:
        temp_dir = "c:\\temp\\public-cache"
        publish_dir = "C:\\data\\corona19-data-archive\\captive-browser"
        capture = SpecializedCapture(temp_dir, publish_dir)

    try:
        print("starting continuous run")
        if capture: special_cases(capture)
        scanner.process()

        def next_time() -> datetime:
            t = datetime.now()
            xmin = t.minute
            if xmin > 20 and xmin < 50:
                t = t + timedelta(hours=1)
                xmin = 5
            else:
                xmin = 35
            t = datetime(t.year, t.month, t.day, t.hour, xmin, 0)
            return t

        cnt = 1
        t = next_time()
        print(f"sleep until {t}")

        while True:
            time.sleep(15.0)
            if datetime.now() < t: continue

            print("==================================")
            print(f"=== run {cnt} at {t}")
            print("==================================")

            try:
                if capture: special_cases(capture)
                scanner.process()
            except Exception as ex:
                logger.exception(ex)
                print(f"run failed, wait 5 minutes and try again")
                t = t + timedelta(minutes=5)

            print("==================================")
            print("")
            t = next_time()
            print(f"sleep until {t}")
            cnt += 1
    finally:
        capture.close()

def main(args_list=None):
    if args_list is None:
        args_list = sys.argv[1:]
    args = parser.parse_args(args_list)

    #main_sheet = "https://docs.google.com/spreadsheets/d/18oVRrHj3c183mHmq3m89_163yuYltLNlOmPerQ18E8w/htmlview?sle=true#"
    
    scanner = PageScanner(args.base_dir, args=args)
    
    if args.continuous:
        run_continuous(scanner)  
    elif args.clean:
        scanner.clean_html()
    else:
        scanner.process()

if __name__ == "__main__":
    main()
