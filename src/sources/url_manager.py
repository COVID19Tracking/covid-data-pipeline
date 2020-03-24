#
# UrlManager
#
#   make sure we don't hit the same URL twice
#

from typing import Tuple
from loguru import logger
import time

from shared.util import fetch_with_requests
from capture.captive_browser import CaptiveBrowser

class UrlManager:

    def __init__(self, headless=True, browser="requests"):
        self.history = {}
        self.size = 0
        self.browser = browser
        self.headless = headless
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

    def fetch_with_captive(self, url: str) -> Tuple[bytes, int]:
        if self._captive == None:
            self._captive = CaptiveBrowser(self.headless, self.browser)
        self._captive.navigate(url)
        if self._captive.has_slow_elements():
            logger.debug(f"  found slow elements, wait for 5 seconds")
            time.sleep(5)
        return self._captive.page_source(), self._captive.status_code()


    def fetch(self, url: str) -> Tuple[bytes, int]:

        if url in self.history:
            return self.history[url]

        if self.browser == "requests":
            content, status = fetch_with_requests(url)
        else:
            content, status = self.fetch_with_captive(url)

        self.history[url] = (content, status)
        if content != None:
            self.size += len(content)
        return content, status

