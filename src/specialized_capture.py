import os
import imageio
import time
from typing import Tuple
from datetime import datetime, timezone
import numpy as np
import shutil
from loguru import logger
import atexit

# change the the imports will work rather than failing mysteriously
from __init__ import check_path
check_path() 

from capture.captive_browser import CaptiveBrowser, are_images_same
from shared.directory_cache import DirectoryCache

from shared.util import get_host
from shared import util_git
from shared import udatetime

class SpecializedCapture():

    def __init__(self, temp_dir: str, publish_dir: str, driver: CaptiveBrowser = None):
        self.temp_dir = temp_dir
        self.publish_dir = publish_dir

        self.cache_images = DirectoryCache(os.path.join(publish_dir, "images"))
        self.cache = DirectoryCache(os.path.join(publish_dir))

        self.changed = False
        self._is_internal_browser = driver is None
        self._browser: CaptiveBrowser = driver

    def get_browser(self) -> CaptiveBrowser:
        if self._browser != None: return self._browser

        logger.info("  [start captive browser]")
        self._browser = CaptiveBrowser()
        atexit.register(self._browser.close)
        return self._browser

    def close(self):
        if self._browser and self._is_internal_browser:
            logger.info("  [stop captive browser]")
            self._browser.close()
            atexit.unregister(self._browser.close)
            self._browser = None

    def publish(self):
        if not self.changed: 
            logger.info("  [nothing changed]")
        else:
            host = get_host()
            dt = datetime.now(timezone.utc)
            msg = f"{udatetime.to_displayformat(dt)} on {host} - Specialized Capture"
            util_git.push(self.publish_dir, msg)

    def remove(self, key: str):
        self.cache.remove(key)

        prefix = f"{key}_"
        for unique_key in self.cache_images.list_files():
            if unique_key == key or unique_key.starts(prefix): 
                self.cache_images.remove(unique_key)


    def screenshot(self, key: str, label: str, url: str):

        logger.info(f"  screenshot {key}")

        ximages_dir = os.path.join(self.temp_dir, "images")
        if not os.path.exists(ximages_dir): os.makedirs(ximages_dir)

        xpath = os.path.join(self.temp_dir,  f"{key}.png")
        xpath_temp = os.path.join(self.temp_dir,  f"{key}_temp.png")
        xpath_prev = os.path.join(self.temp_dir,  f"{key}_prev.png")
        xpath_diff = os.path.join(self.temp_dir,  f"{key}_diff.png")

        browser = self.get_browser()
        if browser == None: raise Exception("Could not get browser")

        logger.info(f"    1. get content from {url}")
        if not browser.navigate(url):
            logger.info("  page timed out -> skip")
        
        logger.info(f"    2. wait for 5 seconds")
        time.sleep(5)

        logger.info(f"    3. save screenshot to {xpath}")
        buffer_new = browser.screenshot(xpath_temp, full_page=True)        
        if buffer_new is None:
            logger.error("      *** could not capture image")
            return

        if os.path.exists(xpath):
            buffer_old = imageio.imread(xpath, as_gray=True)
            is_same, buffer_diff  = are_images_same(buffer_new, buffer_old)
            if is_same:
                logger.info("      images are the same -> return")
                if os.path.exists(xpath_diff): os.remove(xpath_diff)
                return
            else:
                logger.warning("      images are different")
                if os.path.exists(xpath_prev): os.remove(xpath_prev)
                if os.path.exists(xpath): os.rename(xpath, xpath_prev)
                os.rename(xpath_temp, xpath)
                imageio.imwrite(xpath_diff, buffer_diff, format="png")
        else:
            logger.warning("      image is new")
            os.rename(xpath_temp, xpath)

        dt = datetime.now(timezone.utc)
        timestamp = udatetime.to_filenameformat(dt)
        key_image = key + "_" + timestamp + ".png"

        logger.info(f"    4. publish unique image {key_image}")
        xkey_image = self.cache_images.import_file(key_image, xpath)

        # also make a copy in the temp dir so we can preview HTML
        xpath_unique = os.path.join(self.temp_dir, "images", xkey_image)
        shutil.copyfile(xpath, xpath_unique)

        logger.info("    5. publish HTML snippet")
        xpath_html = os.path.join(self.temp_dir, f"{key}.html")
        with open(xpath_html, "w") as f:
            f.write(f"""
    <html>
    <body>
            <h3>{label}</h3>
            <div>captured: {udatetime.to_displayformat(dt)}</div>
            <div>src: <a href='{url}'>{url}</a></div>
            <br />
            <img src='images/{xkey_image}'>
    </body>
    </html>
    """)
        self.cache.import_file(f"{key}.html", xpath_html)
        self.changed = True

def special_cases(capture: SpecializedCapture):

    capture.screenshot("az_tableau", "Arizona Main Page",
        "https://www.azdhs.gov/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/index.php#novel-coronavirus-home"
    )

    # IA has a recaptua
    #capture.screenshot("ia_data", "Arizona Data Page",
    #    "https://idph.iowa.gov/Emerging-Health-Issues/Novel-Coronavirus"
    #)

    capture.screenshot("wy_data", "Wyoming Data Page",
        "https://health.wyo.gov/publichealth/infectious-disease-epidemiology-unit/disease/novel-coronavirus/"
    )

    capture.screenshot("va_tableau", "Virginia Tableau Page",
        "https://public.tableau.com/views/VirginiaCOVID-19Dashboard/VirginiaCOVID-19Dashboard?:embed=yes&:display_count=yes&:showVizHome=no&:toolbar=no"    )

    # screenshot is noisy
    #capture.screenshot("mo_power_bi", "Missouri Power BI",
    #    "https://health.mo.gov/living/healthcondiseases/communicable/novel-coronavirus/"
    #)


if __name__ == "__main__":
    temp_dir = "c:\\temp\\public-cache"
    publish_dir = "C:\\data\\corona19-data-archive\\captive-browser"

    capture = SpecializedCapture(temp_dir, publish_dir)
    try:
        special_cases(capture)
    finally:
        capture.close()
    capture.publish()

