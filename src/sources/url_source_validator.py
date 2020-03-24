# validator for UrlSource

from typing import List, Callable, Dict, Union, Tuple
from loguru import logger
import pandas as pd
import re

from sources.url_source import UrlSource
from shared import udatetime

class UrlSourceValidator():

    def __init__(self):
        self._locations = {}
        self._urls = {}
        self._msg = []

        self.is_valid = False
        self.status_message = ""
        self.num_rows_with_errors = 0
        self.error_messages = []

    def _validate_location(self, location: str):    
        if location == None or location.strip() == "":
            self._msg.append(f"empty location")
        elif not re.match("[._A-Za-z0-9]+", location):
            self._msg.append(f"invalid location")
        elif location.endswith("_data"):            
            self._msg.append(f"location can't end with '_data'")
        else:
            parts = location.split(".")
            if len(parts[0]) != 2:
                self._msg.append(f"expected state abbreviation at start")

    def _validate_source(self, source: str):
        if not re.match("[_A-Za-z0-9]+", source):
            self._msg.append(f"invalid source")

    def _validate_url(self, kind: str, xurl: str):
        if xurl == None or xurl == "": return
        if not re.match("https?://.+", xurl):
            self._msg.append(f"invalid url {kind}: {xurl}")
        if xurl.startswith("https://google.com") or xurl.startswith("https://www.google.com"):
            self._msg.append("google is not allowed as an endpoint")

    def _validate_rows(self, name: str, df: pd.DataFrame) -> bool:

        self._locations = {}

        n = df.shape[0]
        for idx, r in df.iterrows():

            location = r["location"]
            source = r["source_name"]
            main_url = r["main_page"]
            data_url = r["data_page"]
            error_msg = r["error_msg"]

            self._msg.clear()

            # duplicate check
            prev_info = self._locations.get(location)
            if prev_info != None:
                psource, pidx, pmain_url, pdata_url = prev_info
                if main_url == pmain_url and data_url == pdata_url:
                    logger.info(f"  duplicate with {psource}:{pidx}") 
                else:
                    self._msg.append(f"conflict with {psource}:{pidx}")

            info = (source, idx, main_url, data_url)
            self._locations[location] = info

            if error_msg != None and error_msg != "":
                self._msg.append(f"[parser] msg={error_msg}")
            else:
                self._validate_location(location)
                self._validate_source(source)
                self._validate_url("main_page", main_url)
                self._validate_url("data_page", data_url)

            if len(self._msg) > 0:
                self.num_rows_with_errors += 1                
            for m in self._msg:
                self.error_messages.append(f"  {source} {idx} of {n}: {location} - {m}")

        return self.num_rows_with_errors == 0

    def validate(self, source: UrlSource) -> bool:

        self.is_valid = True
        self.status_message = f"Url source {source.name} is valid"

        if source.error_msg != None: 
            self.is_valid = False
            self.status_message = source.error_msg
            return False

        error_msg = self._validate(source)         
        if error_msg != None: 
            logger.error(f"validate returned: {error_msg}")
            source.status = "failed"
            source.error_msg = error_msg
            source.error_at = udatetime.now_as_utc()
            self.is_valid = False            
            self.status_message = f"Url Source {source.name} is not valid"            
            return False

        source.status = "valid"
        source.error_msg = None
        source.error_at = None
        return True

    def _validate(self, source: UrlSource) -> str:

        if source.df is None: 
            return "DataFrame is missing"
        if source.df.shape[0] == 0: 
            return "DataFrame is empty"

        if not "location" in source.df.columns: return "Missing location column"
        if not "main_page" in source.df.columns: return "Missing main_page column"
        if not "data_page" in source.df.columns: return "Missing data_page column"    
        if not "error_msg" in source.df.columns: return "Missing error_msg column"

        if self._validate_rows(source.name, source.df): return None
        return f"{self.num_rows_with_errors} rows are invalid"

    def display_status(self):

        if self.is_valid:
            if self.status_message != None:
                logger.info(self.status_message)
            return

        logger.error(self.status_message)
        for err_msg in self.error_messages:
            logger.error("  " + err_msg)

