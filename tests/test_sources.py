#
# working file for testing the sources  .  should be a unit test
#
import sys
import os
import pandas as pd
from lxml import html
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))


from change_list import ChangeList
from directory_cache import DirectoryCache

from url_source import UrlSource, get_available_sources, dataframe_to_html

urls = {}

# ---- reformat html
def reformat(content: bytes) -> bytes:

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

# ----
def validate_location(location: str):
    pass
def validate_source(location: str):
    pass
def validate_url(kind: str, xurl: str):
    pass

def validate(r: pd.DataFrame):
    location = r["location"]
    source = r["source_name"]
    general_url = r["main_page"]
    data_url = r["data_page"]

    validate_location(location)
    validate_source(source)
    validate_url("main_page", general_url)
    validate_url("data_page", data_url)


def test_load():

    cache = DirectoryCache("c:\\data\\tests\\sources")
    change_list = ChangeList(cache)
    change_list.load()

    sources = get_available_sources()

    for x in sources.items:
        print(f"load {x.name} for {x.endpoint}")
        raw_content = x.fetch()
        df = x.parse(raw_content)         

        key = x.name.lower().replace(" ", "_") + ".html"

        new_content = dataframe_to_html(df) 
        prev_content = cache.read(key)

        changed = False
        if new_content != prev_content:
            cache.write(key, new_content)
            key_raw = key.replace(".html", "_raw." + x.content_type)
            if key_raw.endswith(".html"): raw_content = reformat(raw_content)
            cache.write(key_raw, raw_content)
            changed = True

        if not validate(df):
            change_list.record_failed(key, x.name, x.endpoint, "validation failed")
        elif changed:
            change_list.record_changed(key, x.name, x.endpoint)
        else:
            change_list.record_unchanged(key, x.name, x.endpoint)

if __name__ == "__main__":
    test_load()

