from typing import List, Union
from lxml import html, etree
import re
from loguru import logger


class HtmlCleaner:

    def __init__(self, trace=False):
        self.trace = trace

    def is_guid(self, xid: str) -> bool:
        if xid == None: return False
        return re.search("[0-9a-f]{8}[\\-_][0-9a-f]{4}[\\-_][0-9a-f]{4}[\\-_][0-9a-f]{4}[\\-_][0-9a-f]{12}", xid) != None

    def is_empty(self, elem: html.Element) -> bool:

        if len(elem) > 0: return False

        t = elem.text
        if t == None: return True
        t = t.strip()
        if t == "": return True

        return False

    def regularize_attrib(self, elem: html.Element, n: str):
        v = elem.attrib[n]
        if v == None: return
        
        if n == "id":
            if self.is_guid(v): elem.attrib[n] = "[guid]"
        elif n == "href":
            if v.startswith("#") and self.is_guid(v): 
                elem.attrib[n] = "#"
            elif "urldefensepoint.com" in v:
                elem.attrib[n] = "http://urldefensepoint.com"
            elif v.startswith("https://twitter.com") and elem.text != None and elem.text.startswith("@"):
                elem.getparent().remove(elem)
        elif n == "src":
            if v.startswith("https://www.youtube.com/"):
                elem.attrib[n] = "https://www.youtube.com"

    def clean_attributes(self, elem: html.Element):
        
        to_delete = []
        for n in elem.attrib:
            if n in ["a", "div", "id", "href", "src", "iframe"]:
                self.regularize_attrib(elem, n)
                continue

            if n in ["cellpadding", "cellspacing", "width", "height", "align", "valign", "border"]:
                to_delete.append(n)
                continue
            if n in ["class", "style", "onload", "target", "onmouseout", "onmouseover", "onclick", "onkeydown"]:
                to_delete.append(n)
                continue
            if n in ["role", "scrolling", "tabindex"]:
                to_delete.append(n)
                continue
            if n in ["webpartid", "webpartid2", "allowfullscreen", "rel", "accesskey", "focusable", "bgcolor"]:
                to_delete.append(n)
                continue
            if n.startswith("data-") or n.startswith("aria-"):
                to_delete.append(n)
                continue
            #logger.error(f"unexpected attribute: {n}")

        for n in to_delete:
            del elem.attrib[n]

    def clean_element(self, elem : html.Element):

        tag = elem.tag
        if tag in ["script", "noscript", "style", "meta", "input", "link", "img", "font"]:
            elem.getparent().remove(elem)
            return
        if tag == etree.Comment:
            elem.getparent().remove(elem)
            return
        if tag == etree.ProcessingInstruction:
            elem.getparent().remove(elem)
            return

        if tag == "svg":
            while len(elem): del elem[0]
        else:
            for ch in elem: self.clean_element(ch)

        if tag in ["div", "span"]:
            if self.is_empty(elem):
                elem.getparent().remove(elem)
                return
            elem.tail = None
        elif tag in ["a"]:
            elem.tail = None

        self.clean_attributes(elem)


    def Clean(self, content: Union[bytes,str]) -> bytes:
        " Remove all layout/display informattion from HTML "
        
        if self.trace: logger.info(f"input ===>\n{content}<===\n")

        doc = html.fromstring(content)
        self.clean_element(doc)
        
        out_content = html.tostring(doc, pretty_print=True)
        if type(content) == str:
            out_content = out_content.decode()

        if self.trace: logger.info(f"output ===>\n{out_content}<===\n")
        return out_content



    
