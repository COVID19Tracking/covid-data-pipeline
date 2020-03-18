# url list sources

from typing import List, Callable, Dict, Union, Tuple
from loguru import logger
import pandas as pd
import io

from util import fetch
from directory_cache import DirectoryCache

from url_source_parsers import sources_config

class UrlSource:

    def __init__(self, name: str, endpoint: str, parser: Callable, content_type = "html",
            display_dups: bool = False):
        self.name = name
        self.endpoint = endpoint
        self.parser = parser
        self.content_type = content_type
        self.display_dups = True

        self.content = None
        self.df = None


    def check_result(self, df: pd.DataFrame):
        if df is None or df.shape[0] == 0:
            raise Exception(f"Parser returned no records for source {self.name}")        

        if not "location" in df.columns: raise Exception("Missing location column")
        if not "main_page" in df.columns: raise Exception("Missing main_page column")
        if not "data_page" in df.columns: raise Exception("Missing data_page column")

    def fetch(self) -> bytes:
        logger.info(f"  fetch {self.endpoint}")
        content, status = fetch(self.endpoint)
        if status >= 300:
            raise Exception(f"Could not load {self.endpoint} for source {self.name}")
        if content == None:
            raise Exception(f"Empty content {self.endpoint} for source {self.name}")
        self.content = content
        return content
        
    def parse(self, content: bytes) -> pd.DataFrame:
        logger.info(f"  parse {self.name}")
        df = self.parser(content)
        self.check_result(df)

        df["source_name"] = self.name
        self.df = df
        return self.df

class UrlSources():

    def __init__(self, items: List):
        self.names = []
        self.items = []
        for x in items:            
            y = self.make_source(x)
            self.names.append(y.name)
            self.items.append(y)
    
    def make_source(self, x : Union[List, Dict]) -> UrlSource:
        if type(x) == list:
            content_type, display_dups = "html", False 
            if len(x) == 3:
                name, endpoint, parser = list(x)
            elif len(x) == 4:
                name, endpoint, parser, content_type  = list(x)
            elif len(x) == 5:
                name, endpoint, parser, content_type, display_dups = list(x)
            else:
                raise Exception("Invalid input list, should be: name, endpoint, parser, [content_type], [display_dups]")
        else:
            x = dict(x)
            name, endpoint, parser, content_type, display_dups = \
                x["name"], x["endpoint"], x["parser"], x.get("content_type"), x.get("display_dups")
            if content_type == None: content_type = "html"
            if display_dups == None: display_dups = False
        return UrlSource(name, endpoint, parser, content_type, display_dups)

    def load(self, name: str) -> Tuple[pd.DataFrame, str, bytes]:

        idx = self.names.index(name)
        if idx < 0: raise Exception(f"Invalid name {name}, valid names are {self.names}")

        x = self.items[idx]
        content = x.fetch()
        return x.parse(content), x.endpoint, content

def dataframe_to_html(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_html(buffer)
    return buffer.getvalue().encode()

def get_available_sources() -> UrlSources:
    return UrlSources(sources_config)


