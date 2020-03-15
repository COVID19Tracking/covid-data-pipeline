from typing import List, Union, Dict
from lxml import html, etree
import re
from loguru import logger


class HtmlExtracter:

    def __init__(self, trace: bool = False):
        self.trace = trace

    def load_info(self, attribs: Dict, doc_out: html.Element):

        doc_out.append(html.Element("h3", text=attribs["title"]))
        t = doc_out.append(html.Element("table", attribs={"id":"info", "borders":"1"}))
        for n in attribs:
            if n == "title": continue
            v = attribs[n]
            tr = t.append(html.Element("tr"))
            tr.append(html.Element("td", text=n))
            tr.append(html.Element("td"))
            if v.startswith("http"):
                tr[1].append(html.Element("a", attribs = {"href": v}, text = v))
            else:
                tr[1].text = v
        doc_out.append(html.Element("br"))

    def extract_tables(self, attribs: Dict, doc_out: html.Element):
        pass

    def extract_links(self, attribs: Dict, doc_out: html.Element):
        pass

    def extract_text(self, attribs: Dict, doc_out: html.Element):
        pass

    def extract(self, content: Union[bytes,str], info: Dict) -> bytes:
        " Get Interesting Content from the HTML and reorganize it "
        
        if self.trace: logger.info(f"input ===>\n{content}<===\n")


        doc = html.fromstring(content)
        doc_out = html.fromstring("""
<html>
    <body>
    </body>
<html>
""")
        self.load_info(info, doc_out)
        self.extract_tables(doc, doc_out)
        self.extract_text(doc, doc_out)
        self.extract_links(doc, doc_out)

        out_content = html.tostring(doc_out, pretty_print=True)
        if type(content) == str:
            out_content = out_content.decode()

        if self.trace: logger.info(f"output ===>\n{out_content}<===\n")
        return out_content



    
