import requests
from loguru import logger
from typing import Callable, Tuple

from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait

import imageio
import time
import os
import tempfile
import numpy as np

def are_images_same(buffer1: bytes, buffer2: bytes) -> Tuple[bool, bytes]:

    if buffer1.shape != buffer2.shape:
        return False

    diff = buffer1 - buffer2
    xmin, xmax = diff.min(), diff.max()
    if xmin != xmax: 
        if xmin != 0 and xmax != 255.0:
            scale = 255.0 / (xmax - xmin)
            diff = ((diff - xmin) * scale).astype(np.uint8)
            #h = np.histogram(diff)
            #print(h)
        return False, diff
    else:
        return True, None

class CaptiveBrowser:

    def __init__(self):

        # setup:
        #   1. install firefox
        #   2. install geckodriver: https://github.com/mozilla/geckodriver/releases
        #   3. [windows] install V++ runtime libs: https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads

        # use FireFox. Chrome is jittery
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        self.driver = webdriver.Firefox(options=options)

        self.driver.set_window_size(1366, 2400)
        #options = webdriver.ChromeOptions()
        #options.add_argument('headless')
        #self.driver = webdriver.Chrome(options=options)

    def get(self, url: str) -> bool:
        try:
            self.driver.get(url)
            return True
        except Exception as ex:
            logger.error(ex)
            s = str(ex)
            if "Timeout loading page" in s: return False
            raise ex
    
    def wait(self, secs: int, wait_for: Callable = None):
        w = WebDriverWait(self.driver, secs)
        if wait_for != None:
            w.until(wait_for)

    def page_source(self):
        return self.driver.page_source

# -- out of place
#    def post_to_remote_cache(self, id: str, owner: str, content: bytes):
#        url = f"http://covid19-api.exemplartech.com/cache/{id}?owner={owner}"
#        resp = requests.post(url, data=content, verify=False)
#        if resp.status_code >= 300:
#            logger.error(f"post to cache at {url} failed status={resp.status_code}")
#        return url

    def screenshot(self, xpath: str = None, wait_secs: int = 5, max_retry = 4) -> bytes:
        """
        take a screen shot and return the bytes

        if xpath is None, use a temp file
        if the image is empty, wait for wait_secs seconds and retry.
        retry max_retry times
        
        returns None if it never gets a good image
        """

        if xpath == None:
            xto = os.path.join(tempfile.gettempdir(), "capture_" + tempfile.gettempprefix() + ".png")
        else:
            xto = xpath

        for retry in range(max_retry):

            self.driver.save_screenshot(xto)
            
            if os.path.exists(xto) and os.path.getsize(xto) > 25000: break

            logger.info(f"  [empty screen shot, retry in {wait_secs} - {retry} of {max_retry}")
            time.sleep(wait_secs)

        buffer = imageio.imread(xto, as_gray=True)
        if xpath == None and os.path.exists(xto):
            os.remove(xto)

        return buffer

    def close(self):
        self.driver.close()

