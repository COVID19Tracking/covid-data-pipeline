# reformat an HTML file
from lxml import html

class HTMLFormater():

    def __init__(self):
        pass

    def _indent(self, elem: html.Element, padding: str = "\n"):
        if len(elem) > 0:
            ch_padding = padding + "  "
            if elem.text == None:
                elem.text = ch_padding
            else:
                elem.text = elem.text.strip() + ch_padding

            for ch in elem: self._indent(ch, ch_padding)
            elem[-1].tail = padding
        else:
            if elem.text != None:
                elem.text = elem.text.strip()


    def reformat(self, content: bytes) -> bytes:
        tree = html.fromstring(content)
        self._indent(tree)        
        return html.tostring(tree)
