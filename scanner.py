import os
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

class PageScanner():

    def __init__(self, work_dir:str, main_sheet_url: str):
        self.main_sheet_url = main_sheet_url
        self.cache = DirectoryCache(work_dir) 


    def is_bad_content(self, content: bytes) -> [bool, str]:

        if content == None: return True, "Empty Response"
        if len(content) < 600: return True, f"Response is {len(content)} bytes"
        if re.search(b"Request unsuccessful. Incapsula incident", content):
            return True, f"Site uses Incapsula"
        return False, None

    def fetch_from_sources(self) -> Dict[str, str]:
        " load the google sheet and parse out the individual state URLs"

        logger.info("fetch main page...")
        content, status = self.cache.fetch(self.main_sheet_url)
        if status >= 300:
            logger.error(f"  failed with status={status}")
            return
        self.cache.save(content, "main_sheet.html")

        parser = SheetParser()
        df_config = parser.get_config(content)

        change_list = ChangeList(self.cache)
        change_list.remove_output_files()

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

        def recache_if_changed(state: str, xurl: str) -> bool:

            if xurl == "" or xurl == None or xurl == "None": 
                return

            key = state + ".html"

            #age = self.cache.get_cache_age(key)
            #if age < 5: 
            #    logger.info(f"{key}: skip b/c age < 5 mins")
            #    return
            local_content =  self.cache.load(key)

            remote_content, status = self.cache.fetch(xurl)
            is_bad, msg = self.is_bad_content(remote_content)
            if is_bad:
                change_list.record_failed(key, xurl, msg)
                return False

            if status < 300:
                compare = HTMLCompare(self.cache)
                compare.load_saved_versions(key)
                if compare.is_identical or compare.is_re:                    
                    if compare.is_different(remote_content, local_content):
                        self.cache.save(remote_content, key)
                        change_list.record_needs_check(key, xurl)
                        return True
                    else:
                        change_list.record_unchanged(key, xurl)
                else:
                    self.cache.save(remote_content, key)
                    change_list.record_changed(key, xurl)
                    return False
            else:
                change_list.record_failed(state, xurl, f"HTTP status {status}")
                return False
            return False

        for _, r in df_config.iterrows():
            state = r["State"]
            general_url = clean_url(r["COVID-19 site"])
            data_url = clean_url(r["Data site"])

            if general_url == None:
                logger.warning(f"  no main url for {state}")
                continue
            
            recache_if_changed(state, general_url)
            recache_if_changed(state + "_data", data_url)

            #if idx > 5: break

        change_list.write_text()
        change_list.write_json()
        change_list.write_urls()


# --------------------
def main():

    work_dir = "c:\\Exemplar\\Corona19\\2020-03-08"
    main_sheet = "https://docs.google.com/spreadsheets/d/18oVRrHj3c183mHmq3m89_163yuYltLNlOmPerQ18E8w/htmlview?sle=true#"
    scanner = PageScanner(main_sheet, work_dir)
    scanner.fetch_from_sources()

if __name__ == "__main__":
    main()
