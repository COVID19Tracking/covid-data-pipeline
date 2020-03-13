import re
import requests
import os
import io
import shutil
from loguru import logger
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait

from typing import Callable
from datetime import datetime, timezone

import numpy as np
import imageio
import time

from directory_cache import DirectoryCache
from util import format_datetime_for_file, get_host, save_data_to_github

class CaptiveBrowser:

    def __init__(self):

        # use FireFox. Chrome is jittery
        # https://github.com/mozilla/geckodriver/releases
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        self.driver = webdriver.Firefox(options=options)

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

# ---------------------------

class SpecializedCapture():

    def __init__(self, temp_dir: str, publish_dir: str):
        self.temp_dir = temp_dir
        self.publish_dir = publish_dir

        self.cache_images = DirectoryCache(os.path.join(publish_dir, "images"))
        self.cache = DirectoryCache(os.path.join(publish_dir))

        self.changed = False

        logger.info("  [start captive browser]")
        self.browser = CaptiveBrowser()

    def close(self):
        self.browser.close()

    def publish(self):
        if not self.changed: 
            logger.info("  [nothing changed]")
        else:
            host = get_host()
            dt = datetime.now(timezone.utc)
            msg = f"{dt.isoformat()} on {host} - Specialized Capture"

            save_data_to_github(self.publish_dir, msg)

    def are_images_the_same(self, path1: str, path2: str, out_path: str) -> bool:

        buffer1 = imageio.imread(path1, as_gray=True)
        buffer2 = imageio.imread(path2, as_gray=True)

        diff = buffer1 - buffer2
        xmin, xmax = diff.min(), diff.max()
        if xmin != xmax and xmin != 0 and xmax != 255.0:
            scale = 255.0 / (xmax - xmin)
            diff = ((diff - xmin) * scale).astype(np.uint8)
            h = np.histogram(diff)
            print(h)
            imageio.imwrite(out_path, diff, format="jpg")
            return False
        return True

    def screenshot(self, key: str, label: str, url: str):

        logger.info(f"  screenshot {key}")

        ximages_dir = os.path.join(temp_dir, "images")
        if not os.path.exists(ximages_dir): os.makedirs(ximages_dir)

        xpath = os.path.join(temp_dir,  f"{key}.png")
        xpath_temp = os.path.join(temp_dir,  f"{key}_temp.png")
        xpath_prev = os.path.join(temp_dir,  f"{key}_prev.png")
        xpath_diff = os.path.join(temp_dir,  f"{key}_diff.png")

        logger.info(f"    1. get content from {url}")
        self.browser.get(url)
        
        logger.info("    2. sleep for 5 seconds")
        time.sleep(5)

        logger.info("    3. save screenshot")
        self.browser.save_screenshot(xpath_temp)
    
        if os.path.exists(xpath):
            if self.are_images_the_same(xpath, xpath_temp, xpath_diff):
                logger.info("      images are the same -> return")
                return
            else:
                logger.warning("      images are different")
                if os.path.exists(xpath_prev): os.remove(xpath_prev)
                if os.path.exists(xpath): os.rename(xpath, xpath_prev)
                os.rename(xpath_temp, xpath)
        else:
            logger.warning("      image is new")
            os.rename(xpath_temp, xpath)

        dt = datetime.now(timezone.utc)
        timestamp = format_datetime_for_file(dt)
        key_image = "az_tableau_" + timestamp + ".png"

        logger.info(f"    4. publish unique image {key_image}")
        xkey_image = self.cache_images.import_file(key_image, xpath)

        # also make a copy in the temp dir so we can preview HTML
        xpath_unique = os.path.join(temp_dir, "images", xkey_image)
        shutil.copyfile(xpath, xpath_unique)

        logger.info("    5. publish HTML snippet")
        xpath_html = os.path.join(temp_dir, f"{key}.html")
        with open(xpath_html, "w") as f:
            f.write(f"""
    <html>
    <body>
            <h3>{label}</h3>
            <div>captured: {dt.isoformat()}</div>
            <div>src: <a href='{url}'>{url}</a></div>
            <br />
            <img src='images/{xkey_image}'>
    </body>
    </html>
    """)
        self.cache.import_file(f"{key}.html", xpath_html)
        self.changed = True

if __name__ == "__main__":
    temp_dir = "c:\\temp\\public-cache"
    publish_dir = "C:\\data\\corona19-data-archive\\captive-browser"

    capture = SpecializedCapture(temp_dir, publish_dir)
    capture.screenshot("az_tableau", "Arizona Tableau Page",
        "https://tableau.azdhs.gov/views/COVID-19Table/COVID-19table?:embed=y&:showVizHome=no&:host_url=https%3A%2F%2Ftableau.azdhs.gov%2F&:embed_code_version=3&:tabs=no&:toolbar=no&:showAppBanner=false&:display_spinner=no&iframeSizedToWindow=true&:loadOrderID=0"
    )
    capture.close()
    #capture.publish()

