# url list sources

from typing import List, Callable
from loguru import logger
import io
import pandas as pd

from util import fetch

class UrlSource:

    def __init__(self, name: str, endpoint: str, parser: Callable, display_dups: bool = False):
        self.name = name
        self.endpoint = endpoint
        self.parser = parser
        self.display_dups = True

        self.df = None

    def load(self) -> pd.DataFrame:
        content, status = fetch(self.endpoint)
        if status >= 300:
            raise Exception(f"Could not load {self.endpoint} for source {self.name}")
        if content == None:
            raise Exception(f"Empty content {self.endpoint} for source {self.name}")
        
        self.df = self.parser(content)
        if self.df is None or self.df.shape[0] == 0:
            raise Exception(f"Parser returned no records for source {self.name}")

        return self.df

def parse_google_csv(content: bytes) -> pd.DataFrame:
    df_config = pd.read_csv(io.StringIO(content.decode('utf-8')))
    return df_config

def get_available_sources():
    return [
        UrlSource("google-states", "https://covid.cape.io/states/info.csv", parse_google_csv, display_dups=True)
    ]


