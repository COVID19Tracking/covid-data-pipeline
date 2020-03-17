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

from directory_cache import DirectoryCache
from change_list import ChangeList

from url_manager import UrlManager
from url_source import UrlSource, get_available_sources

from html_cleaner import HtmlCleaner
from html_extracter import HtmlExtracter

from specialized_capture import SpecializedCapture

from util import is_bad_content, get_host, \
    git_pull, git_push, format_datetime_for_log, \
    monitor_start, monitor_check

class DataPipelineConfig():

    def __init__(self, base_dir: str, temp_dir: str, flags : Dict):

        self.base_dir = base_dir
        self.temp_dir = temp_dir
        self.trace = flags["trace"]
        self.capture_image = flags["capture_image"]
        self.rerun_now = flags["rerun_now"]

class DataPipeline():

    def __init__(self, config: DataPipelineConfig):
        
        self.config = config

        self.change_list: ChangeList = None

        base_dir = config.base_dir
        self.cache_raw = DirectoryCache(os.path.join(base_dir, "raw")) 
        self.cache_clean = DirectoryCache(os.path.join(base_dir, "clean")) 
        self.cache_extract = DirectoryCache(os.path.join(base_dir, "extract")) 
        self.cache_diff = DirectoryCache(os.path.join(base_dir, "diff")) 

        self.url_manager = UrlManager()

        self._capture: SpecializedCapture = None 

    def get_capture(self) -> SpecializedCapture:
        if self._capture == None:
            publish_dir = os.path.join(self.config.base_dir, 'captive-browser')
            self._capture = SpecializedCapture(
                self.config.temp_dir, publish_dir)
        return self._capture

    def shutdown_capture(self):
        if self._capture != None:
            self._capture.close()
            if self.config.auto_push:
                self._capture.publish()
        self._capture = None

    def process(self) -> Dict[str, str]:
        " run the pipeline "

        self.url_manager.reset()
        self.change_list = ChangeList(self.cache_raw)        
        
        host = get_host()
        print(f"=== run started on {host} at {format_datetime_for_log(self.change_list.start_date)}")

        self.change_list.start_run()
        try:
            return self._main_loop(self.change_list)
        except Exception as ex:
            logger.exception(ex)
            self.change_list.abort_run(ex)
        finally:
            self.change_list.finish_run()

            self.shutdown_capture()

            logger.info(f"  [in-memory content cache took {self.url_manager.size*1e-6:.1f} MBs")
            logger.info(f"run finished on {host} at {format_datetime_for_log(self.change_list.start_date)}")
            
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

                attributes = {
                    "title": key,
                    "source": f"http://covid19-api.exemplartech.com/source/{key}",
                    "raw": f"http://covid19-api.exemplartech.com/raw/{key}",
                    "clean": f"http://covid19-api.exemplartech.com/clean/{key}"
                }

                extracter = HtmlExtracter()
                local_extract_content = extracter.extract(local_clean_content, attributes)
                self.cache_extract.write(key, local_extract_content)


    def _main_loop(self, change_list: ChangeList) -> Dict[str, str]:

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
                    change_list.temporary_skip(key, xurl, "age < 15 mins")
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

            local_clean_content =  self.cache_clean.read(key)
            cleaner = HtmlCleaner()
            remote_clean_content = cleaner.clean(remote_raw_content)

            if local_clean_content != remote_clean_content:

                self.cache_raw.write(key, remote_raw_content)
                self.cache_clean.write(key, remote_clean_content)
                change_list.record_changed(key, source, xurl)

                attributes = {
                    "title": f"Data for {location} (source={source})",
                    "source": xurl,
                    "raw": f"http://covid19-api.exemplartech.com/raw/{key}",
                    "clean": f"http://covid19-api.exemplartech.com/clean/{key}",
                    "changed_at": change_list.last_timestamp                    
                }
                extracter = HtmlExtracter()
                remote_extract_content = extracter.extract(remote_clean_content, attributes)
                self.cache_extract.write(key, remote_extract_content)


                if self.config.capture_image:
                    c = self.get_capture()
                    c.screenshot(key, f"Screenshot for {location}", xurl)
            else:
                change_list.record_unchanged(key, source, xurl)
                return False

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
            

        change_list.write_html_to_cache(self.cache_raw, "RAW")
        change_list.write_html_to_cache(self.cache_clean, "CLEAN")
        change_list.write_html_to_cache(self.cache_extract, "EXTRACT")
