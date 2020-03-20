# reformat an HTML file
from lxml import html
from loguru import logger

padding = ["\n" + "  "*n for n in range(50)]
padding.insert(0, "")

class HtmlFormater():

    def __init__(self):
        pass

    def _indent_text(self, text: str, depth: int) -> str:
        if text == None: return padding[depth]
        text = text.strip()
        if text == "": return padding[depth]
        parts = text.split("\n")
        if len(parts) == 1: return text + padding[depth]
        parts.insert(0, "")
        parts.append("")
        return padding[depth+2].join(parts)

    def _indent_elem(self, elem: html.Element, depth: int):
        if len(elem) > 0:
            elem.text = self._indent_text(elem.text, depth+1)
            for ch in elem: 
                self._indent_elem(ch, depth+1)
            elem[-1].tail = self._indent_text(elem.tail, depth)
            elem.tail = self._indent_text(elem.tail, depth)
        else:
            elem.text = self._indent_text(elem.text, 0)
            elem.tail = self._indent_text(elem.tail, depth)

    def _inject_extra_elements(self, tree: html.Element, xurl: str):
        if xurl == None: return
        
        if len(tree) == 0 or tree[0].tag != "head":
            return

        base = tree.findall("base")
        if len(base) > 0: return

        base = html.Element("base")
        base.attrib["ref"] = xurl
        tree.insert(0, base)

    def format(self, xurl: str, content: bytes) -> bytes:
        tree = html.fromstring(content)
        self._inject_extra_elements(tree, xurl)
        self._indent_elem(tree, 1)        
        return html.tostring(tree)
