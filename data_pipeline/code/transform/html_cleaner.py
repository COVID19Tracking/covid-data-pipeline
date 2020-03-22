from typing import List, Union
from lxml import html, etree
import re
from loguru import logger


class HtmlCleaner:

    def __init__(self, trace=False):
        self.trace = trace
        self.to_remove = []

    def mark_special_case(self, elem: html.Element) -> bool:
        " edit or return element to remove "
        # -- stupid special cases for CA
        #if elem.tag == "div":
        #    xid = elem.get("id")
        #    if xid == "DeltaPlaceHolderPageDescription" or xid == "DeltaPlaceHolderPageTitleInTitleArea": 
        #        logger.debug("special case: remove deltaplaceholder")
        #        self.to_remove.append(elem.getparent())
        #        return True
        #elif elem.tag == "a":        
        #    href = elem.get("href")
        #    if href == "#ctl00_ctl65_SkipLink": 
        #        logger.debug("special case: remove skiplink")
        #        self.to_remove.append(elem.getparent())
        #        return True

        if elem.tag == "div":
            xid = elem.attrib.get("id")
            if xid == "google_translate_element" and len(elem)>0:
                logger.debug("special case: google_translate_element")
                return elem[0]
            if xid != None:
                xid2 = re.sub("^[0-9a-fA-F]+-(.*)",  "\\1", xid) 
                xid2 = re.sub("(.*)-[a-z]?[0-9a-fA-F]+$",  "\\1", xid2) 
                if xid != xid2: 
                    logger.debug("special case: hex data in id")
                    elem.attrib["id"] = xid2

            if elem.attrib.get("fb-xfbml-state"):
                logger.debug("special case: fb")
                return elem


        return False

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
        if tag in ["script", "noscript", "style", "meta", "input", "iframe", "select", "link", "font"]:
            elem.getparent().remove(elem)
            return
        if tag == etree.Comment:
            elem.getparent().remove(elem)
            return
        if tag == etree.ProcessingInstruction:
            elem.getparent().remove(elem)
            return

        if tag == "form":
            a = elem.attrib.get("action")
            if a != None: del elem.attrib["action"] 
            x = elem.attrib.get("onsubmit")
            if x != None: del elem.attrib["onsubmit"] 

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
            if self.mark_special_case(elem):
                return
        elif tag in ["a"]:
            if self.mark_special_case(elem):
                return
            
            # strip spaces from simple links
            if len(elem) > 0:
                elem[-1].tail = None
            elif elem.text != None:
                elem.text = elem.text.strip()

        self.clean_attributes(elem)


    def clean(self, content: Union[bytes,str]) -> bytes:
        " Remove all layout/display informattion from HTML "
        
        try:
            return self._clean(content)
        except Exception as ex:
            logger.exception(ex)
            logger.error("clean failed")
            return None



    def _clean(self, content: Union[bytes,str]) -> bytes:
        if self.trace: logger.info(f"input ===>\n{content}<===\n")

        self.to_remove = []

        if content == None: return b''

        doc = html.fromstring(content)
        self.clean_element(doc)

        for x in self.to_remove:
            p = x.getparent()
            if p != None: 
                p.remove(x)
            #else:
            #    for e in x: x.remove(e)
            #    doc = x

        if len(doc) == 0:
            logger.warning("  cleaned document is empty")
        for x in doc:
            if x.tag == "body":
                if len(x) == 0:
                    logger.warning("  cleaned document's body is empty")

        try:
            out_content = html.tostring(doc)
        except Exception as ex:
            logger.error(ex)
            logger.error("lxml failed on converting document to text")
            return b''

        if type(content) == str:
            out_content = out_content.decode()

        if self.trace: logger.info(f"output ===>\n{out_content}<===\n")
        return out_content



    
