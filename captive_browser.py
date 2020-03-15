import requests
from loguru import logger
from typing import Callable

from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait

class CaptiveBrowser:

    def __init__(self):

        # use FireFox. Chrome is jittery
        # https://github.com/mozilla/geckodriver/releases
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        self.driver = webdriver.Firefox(options=options)

        self.driver.set_window_size(1366, 2400)
        #options = webdriver.ChromeOptions()
        #options.add_argument('headless')
        #self.driver = webdriver.Chrome(options=options)

    def get(self, url: str):
        self.driver.get(url)
    
    def wait(self, secs: int, wait_for: Callable = None):
        w = WebDriverWait(self.driver, secs)
        if wait_for != None:
            w.until(wait_for)

    def page_source(self):
        return self.driver.page_source

    def post_to_remote_cache(self, id: str, owner: str, content: bytes):
        url = f"http://covid19-api.exemplartech.com/cache/{id}?owner={owner}"
        resp = requests.post(url, data=content, verify=False)
        if resp.status_code >= 300:
            logger.error(f"post to cache at {url} failed status={resp.status_code}")
        return url

    def save_screenshot(self, xpath: str):
        self.driver.save_screenshot(xpath)

    def close(self):
        self.driver.close()

