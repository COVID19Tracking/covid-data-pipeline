from typing import List, Union, Dict
from lxml import html, etree
import re
import json
from loguru import logger
from typing import Tuple
from datetime import datetime

from shared import udatetime
from transform.change_list import ChangeItem
from transform.content_table import ContentTable
from transform.content_text import ContentText, make_content_text

from shared.util import convert_python_to_json

class HtmlConverter:

    def __init__(self, trace: bool = False):
        self.trace = trace

    def convert(self, location: str, content: Union[bytes,str], item: ChangeItem) -> str:
        " Convert extracted HTML into data (json)"
        
        try:
            data = self._convert(location, content, item)
            if data == None: return b'{}'
            convert_python_to_json(data)
            return json.dumps(data).encode()
        except Exception as ex:
            logger.exception(ex)
            logger.error("extract failed")
            return None

    def _convert(self, location: str, content: Union[bytes,str], item: ChangeItem) -> Dict:

        if self.trace: logger.info(f"convert ===>\n{content}<===\n")

        if content == None or len(content) == 0: return {}

        doc = html.fromstring(content)
        if location == "GA.html": 
            return self.convert_ga(doc)
                 
        links = doc.get_element_by_id("links")
        if links != None: 
            links.getparent().remove(links)

        tables = doc.findall(".//table")        
        if len(tables) > 0:
            logger.warning(f"found tables for {location}") 
            for t in tables:
                logger.info(f"  table ===>\n{html.tostring(t, pretty_print=True)}<===\n")
            exit(-1)
            return { "error": "found tables w/o parser", "at": udatetime.now_as_utc() }
        else: 
            return { "error": "no tables", "at": udatetime.now_as_utc() }


    def convert_ga(self, doc: html.Element) -> Dict:

        t = doc.findall(".//table")
        if len(t) == 0: 
            return { "error": "no tables -> page layout changed", "at": udatetime.now_as_utc() }

        data = self._htmltable_to_dict(t[0])
        if len(data["data"]) != 2:
            return { "error": "expected two data rows", "at": udatetime.now_as_utc() }
        if data["data"][0]["COVID-19 Confirmed Cases"] != "Total":
            return { "error": "first row should be totals", "at": udatetime.now_as_utc() }
        if data["data"][1]["COVID-19 Confirmed Cases"] != "Deaths":
            return { "error": "second row should be deaths", "at": udatetime.now_as_utc() }

        positive = data["data"][0]["No. Cases (%)"]
        positive = int(positive[0: positive.index("(")])
        deaths = data["data"][1]["No. Cases (%)"]
        deaths = int(deaths[0: deaths.index("(")])

        data = self._htmltable_to_dict(t[1])
        if len(data["data"]) != 2:
            return { "error": "expected two data rows", "at": udatetime.now_as_utc() }
        if data["data"][0]["Lab"] != "Commercial Lab":
            return { "error": "first row should be Commerial Lab", "at": udatetime.now_as_utc() }
        if data["data"][1]["Lab"] != "GPHL":
            return { "error": "second row should be GPHL", "at": udatetime.now_as_utc() }
        lab_1 = int(data["data"][0]["Total Tests"])
        lab_2 = int(data["data"][1]["Total Tests"])
        tests = lab_1 + lab_2

        return {
            "positive": positive,
            "tests": tests,
            "deaths": deaths
        }


    def _htmltable_to_dict(self, table: etree) -> Dict:
        " converts an html table into a dictionary"
        names = []
        data = []
        caption = None
        cnt = 0

        for elem in table:
            if elem.tag == "caption":
                if len(elem) == 0: 
                    caption = elem.text                        
                else:
                    caption = html.tostring(elem)
                continue

            if cnt == 0:
                for col in elem:
                    names.append(col.text)
            else:
                i = 0
                row = {}
                for col in elem:
                    if len(col) == 0: 
                        val = col.text                        
                    else:
                        val = html.tostring(col)
                    n = names[i]
                    row[n] = val 
                    i += 1
                data.append(row)                
            cnt += 1

        result = {
            "caption": caption,
            "data": data
        }
        return result

