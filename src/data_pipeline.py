"""
data pipeline

1. scan the COVID-19 government sites
2. fetch raw hmtl
3. clean out layout info
4. push to a git repo

files are only updated if the cleaned version changes

"""

import os
from loguru import logger
from typing import List, Dict, Tuple
import pandas as pd

from shared.directory_cache import DirectoryCache
from transform.change_list import ChangeList

from sources.url_manager import UrlManager

from sources.url_source import UrlSource, UrlSources
from sources.url_source_manager import UrlSourceManager

from transform.html_formater import HtmlFormater
from transform.html_cleaner import HtmlCleaner
from transform.html_extracter import HtmlExtracter
from transform.html_converter import HtmlConverter

from specialized_capture import SpecializedCapture

from shared.util import is_bad_content, get_host
from shared import util_git
from shared import udatetime

class DataPipelineConfig():

    def __init__(self, base_dir: str, temp_dir: str, flags : Dict):

        self.base_dir = base_dir
        self.temp_dir = temp_dir
        self.trace = flags["trace"]
        self.capture_image = flags["capture_image"]
        self.rerun_now = flags["rerun_now"]

        self.headless = flags["headless"]

        if flags.get("firefox"):
            self.browser = "firefox"
        elif flags.get("chrome"):
            self.browser = "chrome"
        else:
            self.browser = "requests"

class DataPipeline():

    def __init__(self, config: DataPipelineConfig):
        
        self.config = config

        self.change_list: ChangeList = None

        base_dir = config.base_dir

        self.cache_sources = DirectoryCache(os.path.join(base_dir, "sources")) 

        self.cache_raw = DirectoryCache(os.path.join(base_dir, "raw")) 
        self.cache_clean = DirectoryCache(os.path.join(base_dir, "clean")) 
        self.cache_extract = DirectoryCache(os.path.join(base_dir, "extract")) 
        self.cache_convert = DirectoryCache(os.path.join(base_dir, "convert")) 

        self.cache_diff = DirectoryCache(os.path.join(base_dir, "diff")) 

        self.url_manager = UrlManager(config.headless, config.browser)

        self.sources: UrlSources = None

        self._capture: SpecializedCapture = None 

    def get_capture(self) -> SpecializedCapture:
        if self._capture == None:
            publish_dir = os.path.join(self.config.base_dir, 'captive-browser')
            driver = self.url_manager._captive.driver if self.url_manager._captive else None
            self._capture = SpecializedCapture(
                self.config.temp_dir, publish_dir, driver)
        return self._capture

    def shutdown_capture(self):
        if self._capture != None:
            self._capture.close()
            if self.config.auto_push:
                self._capture.publish()
        self._capture = None

    def update_sources(self):
        " update the remote url sources "
        manager = UrlSourceManager(self.cache_sources)
        self.sources = manager.update_sources("scan")

    def process(self) -> Dict[str, str]:
        " run the pipeline "

        self.url_manager.reset()
        self.change_list = ChangeList(self.cache_raw)        
        
        host = get_host()
        print(f"=== run started on {host} at {udatetime.to_logformat(self.change_list.start_date)}")

        self.change_list.start_run()
        try:
            if self.sources == None:
                raise Exception("Sources not provided")
            src = self.sources.items[0]
            if src.name != "google-states-csv":
                raise Exception(f"Expected first source to be google-states-csv, not {src.name}")
            return self._main_loop(src, self.change_list)
        except Exception as ex:
            logger.exception(ex)
            self.change_list.abort_run(ex)
        finally:
            self.change_list.finish_run()

            self.shutdown_capture()

            logger.info(f"  [in-memory content cache took {self.url_manager.size*1e-6:.1f} MBs")
            logger.info(f"run finished on {host} at {udatetime.to_logformat(self.change_list.start_date)}")
            
    def format_html(self, rerun=False):
        " format raw html "
        is_first = False
        for key in self.cache_raw.list_html_files():
            if key == "index.html": continue
            if key == "google_sheet.html": continue
            if rerun or not self.cache_raw.exists(key):
                if is_first:
                    logger.info(f"format existing files...")
                    is_first = False
                logger.info(f"  format {key}")
                local_raw_content =  self.cache_raw.read(key)
                formater = HtmlFormater()
                local_clean_content = formater.format(None, local_raw_content)
                self.cache_raw.write(key, local_clean_content)

    def clean_html(self, rerun=False):
        " generate clean files from existing raw html "
        is_first = False
        for key in self.cache_raw.list_html_files():
            if key == "index.html": continue
            if key == "google_sheet.html": continue
            if rerun or not self.cache_clean.exists(key):
                if is_first:
                    logger.info(f"clean existing files...")
                    is_first = False
                logger.info(f"  clean {key}")
                local_raw_content =  self.cache_raw.read(key)
                cleaner = HtmlCleaner()
                local_clean_content = cleaner.clean(local_raw_content)
                self.cache_clean.write(key, local_clean_content)


    def extract_html(self, rerun=False):
        " generate extract files from existing clean html "

        self.change_list = ChangeList(self.cache_raw)                
        self.change_list.load()

        is_first = False
        for key in self.cache_clean.list_html_files():
            if key == "index.html": continue
            if key == "google_sheet.html": continue

            if rerun or not self.cache_extract.exists(key):
                if is_first:
                    logger.info(f"extract existing files...")
                    is_first = False
                logger.info(f"  extract {key}")
                local_clean_content =  self.cache_clean.read(key)

                item = self.change_list.get_item(key)
                if item == None:
                    logger.warning("   skip because it is a new item")
                    continue

                extracter = HtmlExtracter()
                local_extract_content = extracter.extract(local_clean_content, item)
                self.cache_extract.write(key, local_extract_content)

    def convert_to_json(self, rerun=False):
        " get json data out of extracted html "

        self.change_list = ChangeList(self.cache_raw)                
        self.change_list.load()

        is_first = False
        for key in self.cache_extract.list_html_files():
            if key == "index.html": continue
            if key == "google_sheet.html": continue

            xkey = key.replace(".html", ".json")
            if rerun or not self.cache_convert.exists(xkey):
                if is_first:
                    logger.info(f"convert existing files...")
                    is_first = False
                logger.info(f"  convert {key}")
                local_extract_content =  self.cache_extract.read(key)

                item = self.change_list.get_item(key)
                if item == None:
                    logger.warning("   skip because it is a new item")
                    continue

                converter = HtmlConverter()
                local_convert_content = converter.convert(key, local_extract_content, item)
                self.cache_convert.write(xkey, local_convert_content)

    def _main_loop(self, source: UrlSource, change_list: ChangeList) -> Dict[str, str]:

        def remove_duplicate_if_exists(location: str, source: str, other_state: str):
            key = location + ".html"

            self.cache_raw.remove(key)
            self.cache_clean.remove(key)
            change_list.record_duplicate(key, source, f"duplicate of {other_state}")

            if self.config.capture_image:
                c = self.get_capture()
                c.remove(location)


        def fetch_if_changed(location: str, source: str, xurl: str, skip: bool = False) -> bool:

            key = location + ".html"

            if xurl == "" or xurl == None or xurl == "None": 
                change_list.record_skip(key, source, xurl, "missing url")
                return

            mins = change_list.get_minutes_since_last_check(key)
            if self.config.trace: logger.info(f"  checked {key} {mins:.1f} minutes ago")
            if mins < 15.0: 
                if self.config.rerun_now:
                    logger.info(f"{key}: checked {mins:.1f} mins ago")
                else:
                    logger.info(f"{key}: checked {mins:.1f} mins ago -> skip b/c < 15 mins")
                    change_list.temporary_skip(key, source, xurl, "age < 15 mins")
                    return False

            if skip:
                change_list.record_skip(key, source, xurl, "skip flag set")
                return False

            if self.config.trace: logger.info(f"fetch {xurl}")
            remote_raw_content, status = self.url_manager.fetch(xurl)
            
            is_bad, msg = is_bad_content(remote_raw_content)
            if is_bad:
                change_list.record_failed(key, source, xurl, msg)
                return False

            if status > 300:
                change_list.record_failed(location, source, xurl, f"HTTP status {status}")
                return False

            remote_raw_content = remote_raw_content.replace(b"\r", b"")

            formater = HtmlFormater()
            remote_raw_content = formater.format(xurl, remote_raw_content)

            local_clean_content =  self.cache_clean.read(key)
            cleaner = HtmlCleaner()
            remote_clean_content = cleaner.clean(remote_raw_content)

            if local_clean_content != remote_clean_content:

                self.cache_raw.write(key, remote_raw_content)
                self.cache_clean.write(key, remote_clean_content)
                change_list.record_changed(key, source, xurl)

                item = change_list.get_item(key)

                formatter = HtmlFormater()
                remote_raw_content = formatter.format(xurl, remote_raw_content)

                extracter = HtmlExtracter()
                remote_extract_content = extracter.extract(remote_clean_content, item)
                self.cache_extract.write(key, remote_extract_content)

                converter = HtmlConverter()
                remote_convert_content = converter.convert(key, remote_extract_content, item)
                self.cache_convert.write(key, remote_convert_content)


                if self.config.capture_image:
                    c = self.get_capture()
                    c.screenshot(key, f"Screenshot for {location}", xurl)
            else:
                change_list.record_unchanged(key, source, xurl)
                return False

        # -- get urls to hit
        if source.status != "valid":
            raise Exception(f"URL source {source.name} status is not valid")

        df_config = source.df
        if df_config is None:
            raise Exception(f"URL source {source.name} does not have any data loaded")

        # -- fetch pages
        skip = False
        err_cnt = 0

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
                try:
                    fetch_if_changed(location, source, general_url, skip=skip)
                except Exception as ex:
                    err_cnt += 1
                    if err_cnt > 10: break
                    change_list.record_failed(location, source, general_url, "Exception in code")
                    logger.exception(ex)
                    logger.error("    error -> continue to next page")

            if data_url != None:
                if general_url == data_url:
                    remove_duplicate_if_exists(location + "_data", source, location)
                else:
                    try:
                        fetch_if_changed(location + "_data", source, data_url, skip=skip)
                    except Exception as ex:
                        err_cnt += 1
                        if err_cnt > 10: break
                        change_list.record_failed(location, source, general_url, "Exception in code")
                        logger.exception(ex)
                        logger.error("    error -> continue to next page")

        if err_cnt > 10:
            logger.error(f"  abort run due to {err_cnt} errors")        

        change_list.write_html_to_cache(self.cache_raw, "RAW")
        change_list.write_html_to_cache(self.cache_clean, "CLEAN")
        change_list.write_html_to_cache(self.cache_extract, "EXTRACT")
