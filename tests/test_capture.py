import sys
import os

from capture.captive_browser import CaptiveBrowser

def test_auto_resize():

    browser = CaptiveBrowser(headless=False, browser="firefox", full_page=True)
    browser.navigate("https://coronavirus.dc.gov/page/coronavirus-data")
    browser.expand_to_full_page()
    browser.screenshot("c:\\temp\\test.png")
    browser.restore_to_original_size()

if __name__ == "__main__":
    test_auto_resize()