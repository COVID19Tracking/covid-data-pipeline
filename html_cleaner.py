from typing import List, Union
from lxml import html, etree
import re
from loguru import logger


class HtmlCleaner:

    def __init__(self, trace=False):
        self.trace = trace
        self.to_remove = []

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
            elif v.startswith("https://www.google.com/url?q="):
                if self.trace: logger.info(f"google >>{v}<<")
                idx = v.find("&ust=")
                if idx > 0: 
                    if self.trace: logger.info("removed ust")
                    elem.attrib[n] = v[0:idx]
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

    def remove_twitter_cluster(self, elem: html.Element):

        if self.trace: logger.info(f"  twitter: check")

        # check it
        if elem.tag != "span": 
            if self.trace: logger.info(f"  twitter: not span")
            return
        for e in elem:
            if e.tag != "a": 
                if self.trace: logger.info(f"  twitter: child not a")
                return
            href = e.attrib["href"]
            if href == None: 
                if self.trace: logger.info(f"  twitter: child no href")
                return
            if not (href.startswith("http://twitter.com") or
                href.startswith("https://twitter.com") or 
                href.startswith("https://t.co")):
                if self.trace: logger.info(f"  twitter: child bad link")
                return
        p = elem.getparent()
        if len(p) != 1 and len(p) != 2:
            if self.trace: logger.info(f"  twitter: parent length ({len(p)})")
            return 
        p = p.getparent()
        if len(p) != 1 and len(p) != 2:
            if self.trace: logger.info(f"  twitter: parent.parent length ({len(p)})")
            return 

        elem_next = p[len(p)-1]
        if elem_next == None: 
            if self.trace: logger.info(f"  twitter: missing next")
            return 

        text = html.tostring(elem_next)
        if self.trace: logger.info(f"  twitter next >>{text}<<")
        if not b"> ago" in text:
            if self.trace: logger.info(f"  twitter: missing ago")
            return 

        # mark it for removal
        if self.trace: logger.info(f"  twitter: remove")
        p.text = ""
        for e in p: self.to_remove.append(e)


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

        if tag == "a":
            href = elem.attrib.get("href")
            if href != None and (
                href.startswith("https://twitter.com") or 
                href.startswith("http://twitter.com") or
                href.startswith("https://t.co")
                ):                
                self.remove_twitter_cluster(elem.getparent())

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


    def clean(self, content: Union[bytes,str]) -> bytes:
        " Remove all layout/display informattion from HTML "
        
        if self.trace: logger.info(f"input ===>\n{content}<===\n")

        self.to_remove = []

        doc = html.fromstring(content)
        self.clean_element(doc)

        for x in self.to_remove:
            p = x.getparent()
            if p != None: 
                p.remove(x)
            #else:
            #    for e in x: x.remove(e)
            #    doc = x

        out_content = html.tostring(doc, pretty_print=True)
        if type(content) == str:
            out_content = out_content.decode()

        if self.trace: logger.info(f"output ===>\n{out_content}<===\n")
        return out_content



    
