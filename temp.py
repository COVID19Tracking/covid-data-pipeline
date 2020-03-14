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


def test_twitter():

    cleaner = HtmlCleaner(trace=True)
    s_in = '''
<span>
    <span>
        <a href="http://twitter.com/IDPH">@IDPH</a>
        <a href="https://twitter.com//search?q=%23COVID19">#COVID19</a>
        <a href="https://t.co/eL6ZH7xQaN">https://t.co/eL6ZH7xQaN</a>
    </span>
</span>
<span>
    <span>- <em>12 hours 23 min</em> ago</span>
</span>
'''

    s_out = cleaner.Clean(s_in)


#test_guid()
test_twitter()