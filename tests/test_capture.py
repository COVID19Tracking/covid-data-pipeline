import sys
import os

from capture.captive_browser import CaptiveBrowser

def test_auto_resize():

    browser = CaptiveBrowser(headless=False, browser="firefox")
    browser.navigate("https://coronavirus.dc.gov/page/coronavirus-data")
    browser.screenshot("c:\\temp\\test.png", full_page=True)

if __name__ == "__main__":
    test_auto_resize()