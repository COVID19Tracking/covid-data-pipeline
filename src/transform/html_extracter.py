from typing import List, Union, Dict
from lxml import html, etree
import re
from loguru import logger
from typing import Tuple
from datetime import datetime

from shared import udatetime
from transform.change_list import ChangeItem
from transform.content_table import ContentTable
from transform.content_text import ContentText, make_content_text
from transform import html_helpers

class HtmlExtracter:

    def __init__(self, trace: bool = False):
        self.trace = trace

        self.link_container = html.Element("table")
        self.link_container.attrib["id"] = "links" 
        self.link_container.attrib["class"] = "links"

        tr = self.make_link_row(("kind", "", "text"))
        tr[1].remove(tr[1][0])
        tr[1].text = "link"
        for i in range(3): tr[i].tag = "th"
        self.link_container.append(tr)
        self.link_container.text = "\n      "

        self.table_container = html.Element("div")
        self.table_container.attrib["id"] = "data" 
        self.table_container.attrib["class"] = "data" 
        self.table_container.tail = "\n    "

        self.text_container = html.Element("div")        
        self.text_container.attrib["id"] = "content" 
        self.text_container.attrib["class"] = "content" 
        self.text_container.tail = "\n    "

    def extract_text(self, elem: html.Element) -> str:
        t = elem.text
        t = t.strip() if t != None else ""

        for ch in elem:
            tx = self.extract_text(ch)
            if tx != "": 
                t += " "
                t += tx
        
        tx = elem.tail
        tx = tx.strip() if tx != None else ""
        if tx != "": 
            t += " "
            t += tx
        return t        

    def extract_link(self, elem: html.Element) -> Tuple[str, str, str]:
        href = elem.attrib.get("href")
        if href == None: return None
        if href.startswith("#"): return None


        text = self.extract_text(elem)

        item = "unknown"
        if elem.tag == "a": 
            item = "link"

            # ignore lots of links
            if href.startswith("."): return None
            if href.startswith("/"): return None
            if href.startswith("tel:"): return None
            if "twitter.com" in href: return None
            if "facebook.com" in href: return None
            if "youtube.com" in href: return None

        elif elem.tag == "iframe": 
            item = "frame"

        return item, href, text

    def make_link_row(self, x: Tuple[str, str, str]) -> html.Element:
        kind, link, text = x
        tr = html.Element("tr")
        tr.text = ""
        tr.tail = "\n  "
        td = html.Element("td")
        td.text = kind
        td.tail = ""
        tr.append(td)
        td = html.Element("td")
        td.text = ""
        td.tail = ""
        if link != None:
            a = html.Element("a", href=link)
            a.text = link
            a.tail = ""
            td.append(a)
        tr.append(td)
        td = html.Element("td")
        td.text = text
        td.tail = ""
        tr.append(td)
        tr.tail = "\n      "
        return tr

    def make_info_row(self, n: str, v: str) -> html.Element:
        tr = html.Element("tr")
        tr.text = ""
        tr.tail = "\n      "
        td = html.Element("td")
        td.text = n
        td.tail = ""
        tr.append(td)
        td = html.Element("td")
        td.tail = ""
        tr.append(td)
        
        if type(v) == datetime:
            tr[1].text = udatetime.to_displayformat(v)
        elif type(v) == str and v.startswith("http"):
            a = html.Element("a")
            a.attrib["href"] = v
            a.text = v
            a.tail = ""
            tr[1].append(a)
        else:
            td.text = n
            tr[1].text = v
        return tr

    def make_row(self, items: List[str]) -> html.Element:
        tr = html.Element("tr")
        tr.text = ""
        tr.tail = "\n      "

        for v in items:
            td = html.Element("td")
            if v.startswith("http"):
                a = html.Element("a")
                a.attrib["href"] = v
                a.text = v
                a.tail = ""
                td.append(a)
            else:
                td.text = v
            tr.append(td)
        return tr

    def load_info(self, item: ChangeItem, 
            body: html.Element):
        
        body.text = "\n    "
        h3 = html.Element("h3")
        h3.text = item.name 
        h3.tail = "\n\n    "
        body.append(h3)

        div = html_helpers.make_source_links("extract", item.name, item.source)
        body.append(div)

        body[len(body)-1].tail = "\n    "
        br = html.Element("br")
        br.tail = "\n    "
        body.append(br)

    def indent_element(self, e: html.Element, depth: int, prefix: str = "\n"):
        xprefix = prefix + "  "
        
        t = e.text
        t = t.strip() if t != None else ""

        if len(e) == 0:
            e.text = t
        else:
            if t != "":
                e.text = prefix + t + "\n"
            else:
                e.text = xprefix

        ch = None
        for ch in e:
            ch.tail = xprefix
            self.indent_element(ch, depth+1, xprefix)    
        if ch != None:
            ch.tail = prefix

        if len(e) > 0:
            e[-1].tail = prefix


    def indent_data_table(self, t: html.Element) -> html.Element:
        
        prefix = "\n      "
        xprefix = prefix + "  "
        t.text = xprefix
        t.tail = prefix
        for ch in t:
            if len(ch) > 0:
                self.indent_element(ch, 0, xprefix)            
            ch.tail = xprefix
        t[-1].tail = prefix

    def process_element(self, elem: html.Element):

        if elem.tag == "a" or elem.tag == "iframe":
            x = self.extract_link(elem)
            if x != None:
               tr = self.make_link_row(x)
               self.link_container.append(tr)
        elif elem.tag == "table":
            ct = ContentTable(elem)
            if ct.contains_data():
               t = ct.reformat()
               if t != None:
                   self.indent_data_table(t)
                   if len(self.table_container) == 0:
                       self.table_container.text = "\n      "
                   else:
                       self.table_container.text = "\n      "
                   self.table_container.append(t)
            return
        else:
            ct = make_content_text(elem)
            if ct != None and ct.contains_data():
                div = ct.as_element()
                div.tail = "\n      "
                if len(self.text_container) == 0:
                    self.text_container.tail = "\n    "
                else:
                    self.text_container.text = "\n      "
                self.text_container.append(div)

        for ch in elem:
            self.process_element(ch)


    def extract(self, content: Union[bytes,str], item: ChangeItem) -> bytes:
        " Get Interesting Content from the HTML and reorganize it "
        
        try:
            return self._extract(content, item)
        except Exception as ex:
            logger.exception(ex)
            logger.error("extract failed")
            return None

    def _extract(self, content: Union[bytes,str], item: ChangeItem) -> bytes:

        if self.trace: logger.info(f"input ===>\n{content}<===\n")

        if content == None or len(content) == 0: return b''

        doc = html.fromstring(content)
        self.process_element(doc)

        if len(self.text_container):
            self.text_container[-1].tail = "\n    "
        if len(self.link_container):
            self.link_container[-1].tail = "\n    "

        doc_out = html.fromstring(f"""
<html>
  <head>
    <title>{item.name}</title>
    <link rel="stylesheet" href="../style.css" type="text/css" />
  </head
  <body>
  </body>
<html>
""")
        self.load_info(item, doc_out[0])
        doc_out[0].append(self.table_container)
        doc_out[0].append(self.text_container)
        doc_out[0].append(self.link_container)
        
        out_content = html.tostring(doc_out, pretty_print=True)
        if type(content) == str:
            out_content = out_content.decode()

        if self.trace: logger.info(f"output ===>\n{out_content}<===\n")
        return out_content



    
