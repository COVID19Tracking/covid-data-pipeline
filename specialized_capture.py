import os
import imageio
import time
from datetime import datetime, timezone
import numpy as np
import shutil
from loguru import logger


from captive_browser import CaptiveBrowser
from directory_cache import DirectoryCache

from util import format_datetime_for_file, get_host, save_data_to_github

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

        if buffer1.shape != buffer2.shape:
            return False

        diff = buffer1 - buffer2
        xmin, xmax = diff.min(), diff.max()
        if xmin != xmax and xmin != 0 and xmax != 255.0:
            scale = 255.0 / (xmax - xmin)
            diff = ((diff - xmin) * scale).astype(np.uint8)
            #h = np.histogram(diff)
            #print(h)
            imageio.imwrite(out_path, diff, format="jpg")
            return False
        return True

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
            <div>captured: {dt.isoformat()}</div>
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

