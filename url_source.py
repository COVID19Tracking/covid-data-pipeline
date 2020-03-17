# url list sources

from typing import List, Callable
from loguru import logger
import io
import pandas as pd
import urllib.parse
import json

from util import fetch

from change_list import ChangeList
from directory_cache import DirectoryCache

def clean_google_url(s: str) -> str:
    if s == None or s == "": return None
    if type(s) != str: return None
    idx = s.find("?q=")
    if idx < 0: return s
    idx += 3
    eidx = s.find("&", idx)
    if eidx < 0: eidx = len(s) 
    s = s[idx:eidx]
    s =  urllib.parse.unquote_plus(s)
    return s


class UrlSource:

    def __init__(self, name: str, endpoint: str, parser: Callable, display_dups: bool = False):
        self.name = name
        self.endpoint = endpoint
        self.parser = parser
        self.display_dups = True

        self.df = None

    def check_result(self, df: pd.DataFrame):
        if df is None or df.shape[0] == 0:
            raise Exception(f"Parser returned no records for source {self.name}")        

        if not "location" in df.columns: raise Exception("Missing location column")
        if not "main_page" in df.columns: raise Exception("Missing main_page column")
        if not "data_page" in df.columns: raise Exception("Missing data_page column")

    def load(self) -> pd.DataFrame:
        content, status = fetch(self.endpoint)
        if status >= 300:
            raise Exception(f"Could not load {self.endpoint} for source {self.name}")
        if content == None:
            raise Exception(f"Empty content {self.endpoint} for source {self.name}")
        
        df = self.parser(content)
        self.check_result(df)

        df["source_name"] = self.name
        self.df = df
        return self.df

    def save_if_changed(self, cache: DirectoryCache, change_list: ChangeList):
        buffer = io.StringIO()
        self.df.to_html(buffer)
        new_content = buffer.getvalue().encode()

        key = f"{self.name}.html"
        old_content = cache.read(key)
        if old_content == new_content:
            change_list.record_unchanged(key, self.name, self.endpoint)
        else:
            cache.write(key, new_content)
            change_list.record_changed(key, self.name, self.endpoint)





def parse_google_csv(content: bytes) -> pd.DataFrame:

    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
    #print(f"df = \n{df}")

    try:
        df["location"] = df["state"]
        df["main_page"] = df["covid19Site"].apply(clean_google_url) 
        df["data_page"] = df["dataSite"].apply(clean_google_url) 
    except:
        logger.info("source changed")
        logger.info(f"df = \n{df}")
        raise Exception("google csv changed")

    return df

def parse_urlwatch(content: bytes) -> pd.DataFrame:
    
    recs = json.loads(content)
    df = pd.DataFrame(recs)
    df["location"] = df["name"]
    df["main_page"] = df["url"].apply(clean_google_url) 
    df["data_page"] = ""

    names = {}
    for x in df.itertuples():
        cnt = names.get(x.name)
        if cnt == None: cnt = 0
        names[x.name] = cnt + 1

        if cnt == 1:
            print(f"assign 2nd url for {x.name} to data")
            df.iloc[x.Index, "data_page"] = x.main_page
            df.iloc[x.Index, "main_page"] = ""
            df.iloc[x.Index, "location"] += "_data"
            print(df.iloc[x.Index])
        elif cnt > 1:            
            print(f"scanner does not support {cnt} urls for {x.name} -> ignore")
            df.iloc[x.Index, "main_page"] = ""
            df.iloc[x.Index, "location"] += f"_{cnt}"

    #print(f"df = \n{df}")
    exit(-1)
    
    


def get_available_sources():

    #main_sheet = "https://docs.google.com/spreadsheets/d/18oVRrHj3c183mHmq3m89_163yuYltLNlOmPerQ18E8w/htmlview?sle=true#"

    return [
        UrlSource("google-states", "https://covid.cape.io/states/info.csv", parse_google_csv, display_dups=True),
        UrlSource("urlwatch", "https://covidtracking.com/api/urls", parse_urlwatch, display_dups=False)
    ]


