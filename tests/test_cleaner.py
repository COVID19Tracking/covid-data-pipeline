#
# working file for testing the cleaner.  should be a unit test
#
import sys
import os

from src import check_path
check_path()

from transform.html_cleaner import HtmlCleaner

def try_one(s_in: str):
    cleaner = HtmlCleaner(trace=True)
    cleaner.clean(s_in)

# ------------------------------------------------
def test_guid():
    try_one('''
<div id="ctl00_ctl00_ctl25_g_a3feef12_d0b6_4c10_904d_9c726d142a82">
    XYZ
</div>
''')


# tableau csv
# https://tableau.azdhs.gov/views/COVID-19Dashboard/COVID-19table.csv

# ------------------------------------------------
def test_twitter():
    try_one('''
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
''')

    try_one('''
<span>RT <a href="http://twitter.com/ReadyIllinois">@ReadyIllinois</a><a href="https://t.co/4tkXWjCnxV">https://t.co/4tkXWjCnxV</a></span></span><span>        <span>- <em>55 min 56 sec</em> ago</span></span>
    ''')
    
    try_one('''
  <span>        <span>Businesses and employers: please encourage employees to stay home if they&#8217;re feeling sick. Prevent community spread&#8230; <a href="https://t.co/eubxb2zOtO">https://t.co/eubxb2zOtO</a></span></span><span>        <span>- <em>14 hours 32 min</em> ago</span></span>
    ''')

# ------------------------------------------------
def test_urldefense():

    try_one('''
<ul>
<li><span>People who have general questions about coronavirus disease 2019 can call CO HELP at 303-389-1687 or 1-877-462-2911 for answers in many languages. Email COHELP@RMPDC.org for answers in English.</span></li>
<li>
<span>Coloradans can also visit the</span><span><a href="https://www.google.com/url?q=https://urldefense.proofpoint.com/v2/url?u%3Dhttps-3A__www.cdc.gov_coronavirus_2019-2Dncov_faq.html%26d%3DDwMFaQ%26c%3DsdnEM9SRGFuMt5z5w3AhsPNahmNicq64TgF1JwNR0cs%26r%3Dps-X2zYjdP7VRiDdNzbgRwL7OM4FMFF3UC46SdZWICk%26m%3DXrCCHKhIT71jB4D-Mnxc_lHBYdTC9ABrsKmcosWlxPE%26s%3D254sNkzOKrmw8Gdu6nzX4P-6-cMqNh8XGzb4idXHJkI%26e%3D&amp;sa=D&amp;ust=1584224563845000">&#194;&#160;CDC</a></span><span>&#194;&#160;and</span><span><a href="https://www.google.com/url?q=https://urldefense.proofpoint.com/v2/url?u%3Dhttps-3A__www.colorado.gov_pacific_cdphe_2019-2Dnovel-2Dcoronavirus%26d%3DDwMFaQ%26c%3DsdnEM9SRGFuMt5z5w3AhsPNahmNicq64TgF1JwNR0cs%26r%3Dps-X2zYjdP7VRiDdNzbgRwL7OM4FMFF3UC46SdZWICk%26m%3DXrCCHKhIT71jB4D-Mnxc_lHBYdTC9ABrsKmcosWlxPE%26s%3D-fHGxhwc4D4LzsShTda-5aHQwLq3kYg5hRH-dASDkCk%26e%3D&amp;sa=D&amp;ust=1584224563846000">&#194;&#160;CDPHE</a></span><span>&#194;&#160;websites for more information on best practices.</span>
</li>
<li>
<span>If you have upcoming travel plans, visit the</span><span><a href="https://www.google.com/url?q=https://urldefense.proofpoint.com/v2/url?u%3Dhttps-3A__www.cdc.gov_coronavirus_2019-2Dncov_travelers_index.html%26d%3DDwMFaQ%26c%3DsdnEM9SRGFuMt5z5w3AhsPNahmNicq64TgF1JwNR0cs%26r%3DfQf2K8W1OAJ1DAepL49iDt5a53fcOLIiLOchBjm-eWw%26m%3DBh9td1MBLmyy6jDQmiKFjkWtkw6dJhyKEmV_N4B2S3U%26s%3D0jn17dUUdUlDd6wR85htnU5jPaDozOGx_R1AWWehOck%26e%3D&amp;sa=D&amp;ust=1584224563847000">&#194;&#160;CDC&#226;&#128;&#153;s website</a></span><span>&#194;&#160;for the latest travel information.</span>
</li>
</ul>
''')

def test_ca_placeholder():

    try_one('''
<div>
                                    <h1 id="pageTitle">
                                        <span id="DeltaPlaceHolderPageTitleInTitleArea">
                                            
												<span><a href="#ctl00_PlaceHolderPageTitleInTitleArea_ctl00_SkipLink"></a><span><a href="/programs/cid/dcdc/pages/immunization/ncov2019.aspx">ncov2019</a></span><a id="ctl00_PlaceHolderPageTitleInTitleArea_ctl00_SkipLink"></a></span></span><div id="DeltaPlaceHolderPageDescription">
	
                                            <a href="javascript:;" id="ms-pageDescriptionDiv">
                                                </a>
</div>
''')

# -------------------------
def test_ca_skiplink():
    try_one('''
<div>
    <span><a href="#ctl00_ctl65_SkipLink"></a><span><a href="/Programs/CID/DCDC">Division of Communicable Disease Control</a></span><span> &gt; </span><span><a href="https://www.cdph.ca.gov/Programs/CID/DCDC/_layouts/15/listform.aspx?ListId=%7B6AAD4E4A%2DF5C6%2D484E%2D8D56%2DFD7746CD9D60%7D&amp;PageType=0">Pages</a></span><span> &gt; </span><span><a href="https://www.cdph.ca.gov/Programs/CID/DCDC/_layouts/15/listform.aspx?ListId=%7B6AAD4E4A%2DF5C6%2D484E%2D8D56%2DFD7746CD9D60%7D&amp;PageType=0&amp;RootFolder=%2FPrograms%2FCID%2FDCDC%2FPages%2FImmunization">Immunization</a></span><span> &gt; </span><span>ncov2019</span><a id="ctl00_ctl65_SkipLink"></a></span>
    <span><a href="#ctl00_ctl65_SkipLink"></a><span><a href="/Programs/CID/DCDC">Division of Communicable Disease Control</a></span><span> &gt; </span><span><a href="https://www.cdph.ca.gov/Programs/CID/DCDC/_layouts/15/listform.aspx?ListId=%7B6AAD4E4A%2DF5C6%2D484E%2D8D56%2DFD7746CD9D60%7D&amp;PageType=0">Pages</a></span><span> &gt; </span><span><a href="https://www.cdph.ca.gov/Programs/CID/DCDC/_layouts/15/listform.aspx?ListId=%7B6AAD4E4A%2DF5C6%2D484E%2D8D56%2DFD7746CD9D60%7D&amp;PageType=0&amp;RootFolder=%2Fprograms%2Fcid%2Fdcdc%2Fpages%2Fimmunization">immunization</a></span><span> &gt; </span><span>ncov2019</span><a id="ctl00_ctl65_SkipLink"></a></span>
</div>
''')

# -------------------------
def test_argis():

    #TODO: make cleaner return a list of URLs that need to get followed.
    try_one('''
                                <iframe frameborder="0" src="https://www.arcgis.com/apps/opsdashboard/index.html#/c091b679e7f64fa78628de361f64eb92"></iframe>
''')


if __name__ == "__main__":
    #test_guid()
    #test_twitter()
    #test_urldefense()
    #test_ca_placeholder()
    test_ca_skiplink()