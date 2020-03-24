# url source
#
#   an externally-sources list of urls to scan
#   see url_source_parsers.py for details
#
from typing import List, Callable, Dict, Union, Tuple
from loguru import logger
import pandas as pd
import io

from shared.util import fetch_with_requests
from shared import udatetime
from shared.directory_cache import DirectoryCache
from transform.change_list import ChangeList
from sources.url_source_parsers import sources_config


# ------------------------------------
class UrlSource:

    def __init__(self, name: str, subfolder: str, 
            endpoint: str, parser: Callable, content_type,
            action: str, display_dups: bool = False):

        # from config
        self.name = name
        self.subfolder = subfolder
        self.endpoint = endpoint
        self.parser = parser
        self.content_type = content_type
        self.action = action
        self.display_dups = display_dups
        
        # persistent to per-source files
        self.content = None
        self.df = None

        # persistent to sources.txt
        self.status = None
        self.updated_at = None
        self.error_msg = None
        self.error_at = None

        self.previous = None

        # not persisted
        self.enable_for_run = True

    def reset(self):
        self.df = None
        self.content = None

        self.previous  = (self.status, self.updated_at, self.error_msg, self.error_at)
        self.status, self.updated_at, self.error_msg, self.error_at = (None, None, None, None)

    def restore(self):
        if self.previous == None: return
        self.status, self.updated_at, self.error_msg, self.error_at = self.previous

    def fetch_with_requests(self) -> bytes:
        logger.info(f"  fetch {self.endpoint} for {self.name}")
        try:
            self.status = "fetch"
            self.updated_at = udatetime.now_as_utc()

            content, status = fetch_with_requests(self.endpoint)
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
            if df is None:
                raise Exception("parser did not return a data frame")
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

            key = f"{name}_source.{self.content_type}"
            cache.write(key, self.content)
            change_list.record_changed(name, "source", self.endpoint)
        else:
            change_list.record_unchanged(name, "source", self.endpoint)

    def write_parsed(self, name: str, cache: DirectoryCache):

        key = f"{name}_parsed.txt"
        if self.df is None: 
            cache.remove(key)
        else:
            content = dataframe_to_text(self.df)
            cache.write(key, content)

    def read(self, name: str, cache: DirectoryCache):

        key = f"{name}_data.txt"
        content = cache.read(key)
        if content == None:
            self.df = dataframe_from_text(content)

        key = f"{name}_source.{self.content_type}"
        self.content = cache.read(key)

    def check_mode(self, mode: str) -> bool:

        action_names = ["enabled", "disabled", "test"]

        self.enable_for_run = True
        if not self.action in action_names:
            raise Exception(f"Invalid action ({self.action}), should be one of " + ", ".join(action_names))
        elif self.action == "disabled":
            self.enable_for_run = False
            logger.warning(f"  skipping because action == disabled")
        if mode == "scan":
            if self.action != "enabled":
                self.enable_for_run = False
                logger.warning(f"  skipping because action != enabled ({self.action})")
        elif mode == "test":
            if not self.action in ["test", "enabled"] :
                self.enable_for_run = False
                logger.warning(f"  skipping because action != test ({self.action})")
        else:
            raise Exception(f"Unexpected mode: {mode}")
        return self.enable_for_run

    def update_from_remote(self) -> pd.DataFrame:

        logger.info(f"update from remote {self.name}")

        self.reset()

        content = self.fetch_with_requests()
        if content != None:
            df = self.parse(content)
            if not df is None:
                logger.info(f"  found {df.shape[0]} records")
        else:
            df = None
            logger.info(f"  no content")

        return df
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

        self.update_status()


    def make_source(self, x : Dict) -> UrlSource:
        name, subfolder, endpoint, parser, content_type, action, display_dups = \
            x["name"], x.get("subfolder"), x["endpoint"], x["parser"], x.get("content_type"), \
            x.get("action"), x.get("display_dups")
        if content_type == None: content_type = "html"
        if display_dups == None: display_dups = False
        if action == None: action = ""

        return UrlSource(name, subfolder, endpoint, parser, content_type, action, display_dups)

    def read(self, cache: DirectoryCache, name: str):
        if len(self.names) == 0: raise Exception("No sources")

        content = cache.read(name)
        if content == None: return 

        df = dataframe_from_text(content)
        df.index = df.names
        missing = ~df.index.isin(self.names)
        if len(missing) > 0:
            df.loc[missing, "status"] = "removed"
        df.reset_index(inplace=True, drop=True)
        self.df_status = df


    def write(self, cache: DirectoryCache, name: str):
        if not self.df_status is None:
            content = dataframe_to_text(self.df_status)
            cache.write(name, content)

    def update_status(self):
        items = self.items
        df = pd.DataFrame({
            "names": [x.name for x in items],
            "status": [x.status for x in items],
            "subfolder": [x.subfolder for x in items],
            "endpoint": [x.endpoint for x in items],
            "updated_at": [udatetime.to_displayformat(x.updated_at) for x in items],
        })
        self.df_status = df

# ----------------
def dataframe_from_text(content: bytes) -> pd.DataFrame:
    if content == None: return None

    buffer = io.StringIO(content.decode())
    df = pd.read_csv(buffer, sep = "\t")
    n = df.columns[0]
    if n.startswith("Unnamed:"): del df[n]
    return df

def dataframe_to_text(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.reset_index(inplace=True, drop=True)
    
    df.to_csv(buffer, sep = "\t")
    return buffer.getvalue().encode()

def dataframe_to_html(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_html(buffer)
    return buffer.getvalue().encode()


def load_one_source(name: str) -> UrlSources:
    sources = UrlSources()
    sources.scan(sources_config)
    
    idx = sources.names.index(name)
    if idx < 0: raise Exception("Invalid source name: {name}, should be one of " + ", ".join(sources.names))

    src = sources.items[idx]
    src.update_from_remote()
    return src