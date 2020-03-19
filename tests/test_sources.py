#
# working file for testing the sources  .  should be a unit test
#
import sys
import os
import pandas as pd
from lxml import html
import re
import json
from loguru import logger

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))


from change_list import ChangeList
from directory_cache import DirectoryCache

from url_source import UrlSource, UrlSources, load_one_source
from url_source_validator import UrlSourceValidator
from url_source_manager import UrlSourceManager
from url_source_parsers import sources_config

urls = {}

# ---- reformat content
def reformat_html(content: bytes) -> bytes:
    tree = html.fromstring(content)
    
    def indent(elem: html.Element, padding: str = "\n"):
        if len(elem) > 0:
            ch_padding = padding + "  "
            if elem.text == None:
                elem.text = ch_padding
            else:
                elem.text = elem.text.strip() + ch_padding

            for ch in elem: indent(ch, ch_padding)
            elem[-1].tail = padding
        else:
            if elem.text != None:
                elem.text = elem.text.strip()
    indent(tree)

    return html.tostring(tree)

def reformat_json(content: bytes) -> bytes:

    x = json.loads(content)
    return json.dumps(x, indent = 2).encode()

def reformat(content: bytes, content_type: str) -> bytes:

    if content_type == "csv": return content
    if content_type == "html": return reformat_html(content)
    if content_type == "json": return reformat_json(content)
    raise Exception(f"Unexpected content_type {content_type}")


# ----
def test_load_standalone():

    src = load_one_source("google-states-csv")
    logger.info(f"df =>>\n{src.df}")

def test_load():

    cache = DirectoryCache("c:\\data\\tests\\sources")

    manager = UrlSourceManager(cache)
    manager.update_sources("test")

    content = cache.read("sources.txt")
    if content != None:
        logger.info(f"sources:\n")
        logger.info(content.decode())
    else:
        logger.error("no sources.txt file")


def test_alt():    
    cache = DirectoryCache("c:\\data\\tests\\sources")

    sources = UrlSources()
    sources.scan(sources_config)
    sources.read(cache, "sources.txt")
    
    validator = UrlSourceValidator()
    for x in sources.items:
        print(f"source {x.name} for {x.endpoint}")

        print(f"   validate {x.name} for {x.endpoint}")
        if not validator.validate(x):
            validator.display_status()
            print("  not valid")

        print(f"   reformat and save source data")
        x.read(x.name, cache)
        if x.content == None:
            logger.warning("  could not read content")
        else:
            raw_content = reformat(x.content, x.content_type)
            if raw_content != None:
                key_raw = f"{x.name}_reformated.{x.content_type}"
                cache.write(key_raw, raw_content)

    sources.write(cache, "sources.txt")


if __name__ == "__main__":
    #test_load_standalone()
    test_load()

