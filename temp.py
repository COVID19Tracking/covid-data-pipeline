from lxml import html
from html_cleaner import HtmlCleaner

def test_guid():

    cleaner = HtmlCleaner(trace=True)

    s_in = '''
<div id="ctl00_ctl00_ctl25_g_a3feef12_d0b6_4c10_904d_9c726d142a82">
    XYZ
</div>
'''
    s_out = cleaner.Clean(s_in)


test_guid()