#
# UrlManager
#
#   make sure we don't hit the same URL twice
#

from util import fetch_with_requests
from typing import Tuple
from loguru import logger

from captive_browser import CaptiveBrowser

class UrlManager:

    def __init__(self, browser="requests"):
        self.history = {}
        self.size = 0
        self.browser = browser
        self._captive = None

    def is_repeat(self, url: str) -> bool:
        return url in self.history

    def reset(self):
        self.history = {}
        self.size = 0

    def shutdown(self):
        if self._captive != None:
            self._captive.close()
            self._captive = None

    def fetch_with_firefox(self, url: str) -> Tuple[bytes, int]:
        if self._captive == None:
            self._captive = CaptiveBrowser("firefox")
        self._captive.navigate(url)
        return self._captive.page_source(), self._captive.status_code()

    def fetch_with_chrome(self, url: str) -> Tuple[bytes, int]:
        if self._captive == None:
            self._captive = CaptiveBrowser("chrome")
        self._captive.navigate(url)
        return self._captive.page_source(), self._captive.status_code()


    def fetch(self, url: str) -> Tuple[bytes, int]:

        if url in self.history:
            return self.history[url]

        if self.browser == "requests":
            content, status = fetch_with_requests(url)
        elif self.browser == "firefox":
            content, status = self.fetch_with_firefox(url)
        elif self.browser == "chrome":
            content, status = self.fetch_with_chrome(url)

        self.history[url] = (content, status)
        if content != None:
            self.size += len(content)
        return content, status

