#
# UrlManager
#
#   make sure we don't hit the same URL twice
#

from util import fetch
from typing import Tuple
from loguru import logger

class UrlManager:

    def __init__(self):
        self.history = {}
        self.size = 0

    def fetch(self, url: str) -> Tuple[bytes, int]:

        if url in self.history:
            return self.history[url]

        content, status = fetch(url)
        self.history[url] = (content, status)
        if content != None:
            self.size += len(content)
        return content, status

