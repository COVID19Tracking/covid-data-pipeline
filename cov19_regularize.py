#
#  This model suppresses all the dynamic variation in the state sites
#  by manipulating the DOM.
#
#  The hard states get their own functions but most of the work is generalized.
#
#  It tries to make the minimum set of changes required.
#
#  public function is:
#      text = regularize_text(text)
#

from typing import List, Union
from lxml import html, etree
import re

def safe_starts_with(val: Union[str, None], prefix: str) -> bool:
    if val == None: return False
    return val.startswith(prefix)

def safe_contains(val: Union[str, None], prefix: str) -> bool:
    if val == None: return False
    return prefix in val

def check_title(elem: html.Element, txt: str) -> bool:
    titles = elem.xpath('//title')
    if titles == None: return False
    for t in titles:
        if safe_contains(t.text, txt): return True
    return False

def regularize_if_la(elem: html.Element) -> bool:
    " special case for lousiana "
    
    if not check_title(elem, "Louisiana Department of Health"): return False

    def clobber(xelem: html.Element):
        if "id" in xelem.attrib: del xelem.attrib["id"]
        if "class" in xelem.attrib: del xelem.attrib["class"]
        if "aria-label" in xelem.attrib: del xelem.attrib["aria-label"]

        if xelem.tag == "script":
            xelem.text = "[removed]"
        elif xelem.tag == "link":
            xelem.attrib["href"] = "[removed]"
            xelem.attrib["data-bootloader-hash"] = "[removed]"
        elif xelem.tag == "a":
            xelem.attrib["href"] = "[removed]"
            if "ajaxify" in xelem.attrib: del xelem.attrib["ajaxify"]
        elif xelem.tag == "img":
            xelem.attrib["src"] = "[removed]"

        for ch in xelem: clobber(ch)

    clobber(elem)
    return True

def regularize_if_co_data(elem: html.Element) -> bool:
    " special case for colorado data url "

    if not check_title(elem, "Colorado COVID-19 Fast Facts"): return False

    def clobber(xelem: html.Element):
        if xelem.attrib.get("id"): xelem.attrib["id"] = ""
        if xelem.attrib.get("class"): xelem.attrib["class"] = ""

        if xelem.tag == "script" and xelem.text != None:
            if xelem.attrib.get("nonce") != None:
                xelem.attrib["nonce"] = "[removed]"
                xelem.text = "[removed]"
        elif xelem.tag == "style":
            if xelem.attrib.get("nonce") != None:
                xelem.attrib["nonce"] = "[removed]"
            elif safe_starts_with(xelem.text, ".lst-kix"):
                xelem.text = "[removed]"            
        elif xelem.tag == "img":
            if xelem.attrib["alt"] == "Colorado Public Health logo":
                xelem.attrib["src"] = "[removed]"                 
        elif xelem.tag == "a":
            if safe_contains(xelem.attrib.get("href"), "urldefense.proofpoint.com"):
                xelem.attrib["href"] = "[removed]"

        for ch in xelem: clobber(ch)

    clobber(elem)
    return True

def remove_attribs(elem: html.Element):
    names = [x for x in elem.attrib]
    for n in names: del elem.attrib[n]

def is_guid(xid: str) -> bool:
    if xid == None: return False
    return re.match("[a-zA-Z]*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", xid) != None

def regularize_other(elem: html.Element):
    " other cases "

    if elem.tag == "input":
        # AZ
        if elem.attrib.get("type") == "hidden":
            elem.attrib["value"] = "[removed]"

    elif elem.tag == "div":
        # CA
        if elem.attrib.get("id") == "DeltaFormDigest":
            elem.text = "[removed]"
            while len(elem) > 0: elem.remove(elem[0])
        elif is_guid(elem.attrib.get("id")): 
            elem.attrib["id"] = "[guid]"
        # DC
        elif safe_contains(elem.attrib.get("class"), "view-custom-headers-and-footers"):
            elem.attrib["class"] = "[removed]"
        # IL
        elif safe_starts_with(elem.attrib.get("class"), "view view-tweets"):          
            elem.attrib["class"] = "[removed]"
        # OH
        elif safe_contains(elem.attrib.get("class"), " id-"):          
            elem.attrib["class"] = "[removed]"
    elif elem.tag == "span":
        # IL
        if safe_contains(elem.attrib.get("class"), "views-field-created-time"):
            elem.attrib["class"] = "[removed]"


    elif elem.tag == "script":

        # CO
        if safe_starts_with(elem.text, "jQuery.extend(Drupal.setting"):
            elem.text = "[removed]"
        elif safe_starts_with(elem.text, "window.NREUM"):
            elem.text = "[removed]"
        # DC
        elif safe_starts_with(elem.text, "window.NREUM||(NREUM"):
            elem.text = "[removed]"
        # OH
        elif safe_contains(elem.text, "var WASReqURL = ") or safe_contains(elem.text, "wpModules.theme.WindowUtils"):
            elem.text = "[removed]"
        elif safe_contains(elem.attrib.get("src"), "/wps/contenthandler"):
            elem.attrib["src"] = "/wps/contenthandler"
        # KY
        elif safe_contains(elem.text, "var formDigestElement = "):
            elem.text = "[removed]"
        elif safe_contains(elem.text, "RegisterSod("):
            elem.text = "[removed]"
        # MO and NJ
        elif safe_contains(elem.attrib.get("src"), "_Incapsula_Resource"):
            elem.attrib["src"] = "/_Incapsula_Resource"
        # NC
        elif safe_contains(elem.text, "-jQuery.extend(Drupal.settings, "):
            elem.text = "[removed]"
        # NE
        elif safe_contains(elem.text, "var g_correlationId = '"):
            elem.text = "[removed]"
        # PA
        elif safe_contains(elem.text, "var MSOWebPartPageFormName = 'aspnetForm'"):
            elem.text = "[removed]"
        # RI
        elif safe_contains(elem.text, 'window["blob') or safe_contains(elem.text, 'window["bob'):
            elem.text = "[removed]"
        # TX
        elif safe_starts_with(elem.attrib.get("id"), "EktronScriptBlock"):
            elem.attrib["id"] = "EktronScriptBlock"
            elem.text = "[removed]"
        # main_sheet
        elif elem.attrib.get("nonce") != None:
            elem.attrib["nonce"] = "[removed]"
            if safe_starts_with(elem.text, "_docs_flag_initialData="):
                elem.text = "[removed]"
            if safe_starts_with(elem.text, "document.addEventList"):
                elem.text = "[removed]"
                
    elif elem.tag == "noscript":        
        # RI and WA
        elem.text = ""
        while len(elem) > 0: elem.remove(elem[0])
    elif elem.tag == "meta":
        # CT
        if elem.attrib.get("name") == "VIcurrentDateTime":
            elem.attrib["content"] = "[removed]"
        # CO
        elif elem.attrib.get("property") in ["og:updated_time", "article:modified_time"]:
            elem.attrib["content"] = "[removed]"     
    elif elem.tag == "link":
        # OH
        if safe_starts_with(elem.attrib.get("href"), "/wps/portal/gov"):
            elem.attrib["id"] = "[removed]"
            elem.attrib["href"] = "[removed]"
    elif elem.tag == "a":
        # OH
        if elem.attrib.get("class") == "left-navigation__link":
            elem.attrib["href"] = "[removed]"
            elem.text = ""
        # OR
        elif safe_starts_with(elem.attrib.get("role"), "tab") and is_guid(elem.attrib.get("id")):
            remove_attribs(elem)
    elif elem.tag == "body":
        # KY
        if safe_starts_with(elem.attrib.get("class"), "brwsr-"):
            elem.attrib["class"] = "brwsr-"
    elif elem.tag == "h1" or elem.tag == "h2":
        # CA
        if len(elem) > 0 and elem[0].tag == "br":
            del elem[0]
    elif elem.tag == "style":
        # DC
        if safe_starts_with(elem.text, "/* Global Styles */"):
            elem.text = "[removed]"
        # main_sheet
        elif elem.attrib.get("nonce") != None:
            elem.attrib["nonce"] = "[removed]"

    elif elem.tag == etree.Comment:
        if safe_contains(elem.text, "Michigan"):
            elem.text = "[removed]"


    for ch in elem:
        regularize_other(ch)

def regularize(content: bytes) -> bytes:
    " regularize html content "

    doc = html.fromstring(content)
    if regularize_if_la(doc):
        pass
    elif regularize_if_co_data(doc):
        pass
    else:
        regularize_other(doc)

    content = html.tostring(doc, pretty_print=True)
    return content
