# url source
#
#   an externally-sources list of urls to scan
#   see url_source_parsers.py for details
#
from typing import List, Callable, Dict, Union, Tuple
from loguru import logger
import pandas as pd
import io

from util import fetch
from directory_cache import DirectoryCache
from change_list import ChangeList

from url_source_parsers import sources_config

import udatetime

# ------------------------------------
class UrlSource:

    def __init__(self, name: str, subdir: str, 
            endpoint: str, parser: Callable, content_type = "html",
            display_dups: bool = False):

        # from config
        self.name = name
        self.subdir = subdir
        self.endpoint = endpoint
        self.parser = parser
        self.content_type = content_type
        self.display_dups = True
        
        # persistent to per-source files
        self.content = None
        self.df = None

        # persistent to sources.txt
        self.status = None
        self.updated_at = None
        self.error_msg = None
        self.error_at = None

        self.previous = None

    def reset(self):
        self.df = None
        self.content = None

        self.previous  = (self.status, self.updated_at, self.error_msg, self.error_at)
        self.status, self.updated_at, self.error_msg, self.error_at = (None, None, None, None)

    def restore(self):
        if self.previous == None: return
        self.status, self.updated_at, self.error_msg, self.error_at = self.previous

    def fetch(self) -> bytes:
        logger.info(f"  fetch {self.endpoint} for {self.name}")
        try:
            self.status = "fetch"
            self.updated_at = udatetime.now_as_utc()

            content, status = fetch(self.endpoint)
            if status >= 300:
                raise Exception(f"Could not load {self.endpoint} for source {self.name}")
            if content == None:
                raise Exception(f"Empty content {self.endpoint} for source {self.name}")
            self.content = content
            return content
        except Exception as ex:
            logger.exception(ex)
            logger.error(f"fetch failed, status={status}")
            self.content = None
            self.status = "failed"
            self.error_msg = f"fetch failed, status={status}"
            self.error_at = self.updated_at
            return None

    def parse(self, content: bytes) -> pd.DataFrame:
        logger.info(f"  parse {self.name}")
        try:
            self.status = "parse"

            df = self.parser(content)
            df["source_name"] = self.name
            if not "data_page" in df.columns: df["data_page"] = ""
            if not "error_msg" in df.columns: df["error_msg"] = ""
            if not "comment" in df.columns: df["comment"] = ""
            self.df = df
            return self.df
        except Exception as ex:
            logger.exception(ex)
            logger.error(f"parse failed")
            self.df = None
            self.status = f"failed"
            self.error_msg = f"parse failed"
            self.error_at = self.updated_at
            return None

    def write(self, name: str, cache: DirectoryCache, change_list: ChangeList):

        key = f"{name}_data.txt"
        new_content = dataframe_to_text(self.df)

        old_content = cache.read(key)
        if old_content != new_content:
            cache.write(key, new_content)
            change_list.record_unchanged(name, "source", self.endpoint)

            key = f"{name}_source.{self.content_type}"
            cache.write(key, self.content)
        else:
            change_list.record_changed(name, "source", self.endpoint)

    def read(self, name: str, cache: DirectoryCache):

        key = f"{name}_data.txt"
        content = cache.read(key)
        if content == None:
            self.df = dataframe_from_text(content)

        key = f"{name}_source.{self.content_type}"
        self.content = cache.read(key)

# -------------------------------------------------
class UrlSources():

    def __init__(self):
        self.names = []
        self.items = []

        self.df_status = None

    def scan(self, input_config: List):
        names = []
        items = []
        for x in input_config:            
            y = self.make_source(x)
            names.append(y.name)
            items.append(y)

        logger.info(f"  found {len(names)} sources")
        self.names = names
        self.items = items

        df = pd.DataFrame({"names": names})
        df["status"] = "new"
        df["updated_at"] = udatetime.now_as_utc()
        self.df_status = df


    def make_source(self, x : Union[List, Dict]) -> UrlSource:
        if type(x) == list:
            content_type, display_dups = "html", False 
            if len(x) == 4:
                name, subdir, endpoint, parser = list(x)
            elif len(x) == 5:
                name, subdir, endpoint, parser, content_type  = list(x)
            elif len(x) == 6:
                name, subdir, endpoint, parser, content_type, display_dups = list(x)
            else:
                raise Exception("Invalid input list, should be: name, subdir, endpoint, parser, [content_type], [display_dups]")
        else:
            x = dict(x)
            name, subdir, endpoint, parser, content_type, display_dups = \
                x["name"], x.get("subdir"), x["endpoint"], x["parser"], x.get("content_type"), x.get("display_dups")
            if content_type == None: content_type = "html"
            if display_dups == None: display_dups = False

        return UrlSource(name, subdir, endpoint, parser, content_type, display_dups)

    def read(self, cache: DirectoryCache, name: str):
        if len(self.names) == 0: raise Exception("No sources")

        content = cache.read(name)
        if content == None: return 

        df = dataframe_from_text(content)
        df.index = df.names
        missing = ~df.index.isin(self.names)
        if len(missing) > 0:
            df.loc[missing, "status"] = "removed"
        self.df_status = df

    def write(self, cache: DirectoryCache, name: str):
        if not self.df_status is None:
            content = dataframe_to_text(self.df_status)
            cache.write(name, content)

    def update_from_remote(self, name: str):

        idx = self.names.index(name)
        if idx < 0: raise Exception(f"Invalid name {name}, valid names are {self.names}")

        logger.info(f"update {name}")
        x = self.items[idx]

        x.reset()

        content = x.fetch()
        if content != None:
            df = x.parse(content)
            logger.info(f"  found {df.shape[0]} records")
        else:
            logger.info(f"  no content")

        self.df_status.loc[x.name, "status"] = x.status
        self.df_status.loc[x.name, "updated_at"] = x.updated_at 


# ----------------
def dataframe_from_text(content: bytes) -> pd.DataFrame:
    if content == None: return None

    buffer = io.StringIO(content.decode())
    return pd.read_csv(buffer, sep = "\t")

def dataframe_to_text(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.reset_index(inplace=True, drop=True)
    df.to_csv(buffer, sep = "\t")
    return buffer.getvalue().encode()

def dataframe_to_html(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_html(buffer)
    return buffer.getvalue().encode()


