# URL SOURCE PARSERS
#
# To add a new parser:
#   1. implement a new parse routine.
#   2. add a line to sources_config
#   3. run tests\test_sources to validate it.
#
# The parser routine needs to return data frame with colunmns:
#    location - where this link is for
#    main_page - the page to hit
#    data_page - [optional] a second page to hit (may be blank or same as main_page)
#    error_msg - [optional] an error message from the parser
#    comment - [optional] a comment from the parser
#
# Extra columns are allowed but not used
#
# Location is the identifier.  It controls how the file is stored.  It should be one of: 
#    <state>
#    <state>_<variant>
#    <state>.<subregion>
#    <state>.<subregion>_<variant>
# 
# State should be a two letter code.
#
# The data page is distingushed from the main_page by appending "_data".  This is a holdover
# from the original google worksheet that allows you to have two URLs for a state.
#
# Locations must be be unique within a directory because they map to file names.  Locations
# are allowed to be duplicated across sources if, and only if, they point to the same url.  
# If not, validation will fail and you have to make it unique.
# 
# If there is a duplicate URL, it will only be fetched once.
#
# If the parser returns a column called error_msg, it will be interpreted as per-parser 
# validation error and reported out as part of the run.
# 

import io
import pandas as pd
import urllib.parse
import json
from lxml import html
from loguru import logger

from shared.google_sheet import GoogleSheet

def clean_google_url(s: str) -> str:
    "extract dest from a google query link"
    if s == None or s == "": return None
    if type(s) != str: return None
    idx = s.find("?q=")
    if idx < 0: return s
    idx += 3
    eidx = s.find("&", idx)
    if eidx < 0: eidx = len(s) 
    s = s[idx:eidx]
    s =  urllib.parse.unquote_plus(s)
    return s

# ------------------------------------------
def parse_google_csv(content: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
    print(df.columns)
    df["location"] = df["state"]
    df["main_page"] = df["covid19Site"].apply(clean_google_url) 
    df["data_page"] = df["covid19SiteSecondary"].apply(clean_google_url) 
    return df

# ------------------------------------------

state_abrrevs = {
  "Alabama": "AL",
  "Alaska": "AK",
  "Arizona": "AZ",
  "Arkansas": "AR",
  "California": "CA",
  "Colorado": "CO",
  "Connecticut": "CT",
  "Delaware": "DE",
  "Florida": "FL",
  "Georgia": "GA",
  "Hawaii": "HI",
  "Idaho": "ID",
  "Illinois": "IL",
  "Indiana": "IN",
  "Iowa": "IA",
  "Kansas": "KS",
  "Kentucky": "KY",
  "Louisiana": "LA",
  "Maine": "ME",
  "Maryland": "MD",
  "Massachusetts": "MA",
  "Michigan": "MI",
  "Minnesota": "MN",
  "Mississippi": "MS",
  "Missouri": "MO",
  "Montana": "MT",
  "Nebraska": "NE",
  "Nevada": "NV",
  "New Hampshire": "NH",
  "New Jersey": "NJ",
  "New Mexico": "NM",
  "New York": "NY",
  "North Carolina": "NC",
  "North Dakota": "ND",
  "Ohio": "OH",
  "Oklahoma": "OK",
  "Oregon": "OR",
  "Pennsylvania": "PA",
  "Rhode Island": "RI",
  "South Carolina": "SC",
  "South Dakota": "SD",
  "Tennessee": "TN",
  "Texas": "TX",
  "Utah": "UT",
  "Vermont": "VT",
  "Virginia": "VA",
  "Washington": "WA",
  "West Virginia": "WV",
  "Wisconsin": "WI",
  "Wyoming": "WY",
  "District of Columbia": "DC",
  "Marshall Islands": "MH",
  "Armed Forces Africa": "AE",
  "Armed Forces Americas": "AA",
  "Armed Forces Canada": "AE",
  "Armed Forces Europe": "AE",
  "Armed Forces Middle East": "AE",
  "Armed Forces Pacific": "AP",

# special cases
  "Washington DC": "DC",
  "Commonwealth of the Northern Mariana Islands": "MP",
  "Guam": "GU",
  "Puerto Rico": "PR",
  "Virgin Islands": "VI"
}

def parse_urlwatch(content: bytes) -> pd.DataFrame:
    
    recs = json.loads(content)
    df = pd.DataFrame(recs)
    df.index = df.name

    df["location"] = df.name
    df["main_page"] = df.url.apply(clean_google_url) 
    df["data_page"] = ""
    df["error_msg"] = ""

    # assign 2nd link to data page so we get only one record instead of two
    # for mutiple links, treat it as a variant.
    names = {}
    for x in df.itertuples():
        cnt = names.get(x.name)
        if cnt == None: cnt = 0
        names[x.name] = cnt + 1

        if cnt == 1:
            df.iloc[x.Index, "data_page"] = x.main_page
            df.iloc[x.Index, "main_page"] = ""
            df.iloc[x.Index, "location"] += "_data"
        elif cnt > 1:
            df.iloc[x.Index, "main_page"] = ""
            df.iloc[x.Index, "location"] += f"_{cnt}"

    # apply state abbreviations
    df["abbrev"] = df.name.map(state_abrrevs)
    missing = pd.isnull(df.abbrev)
    df.loc[~missing, "location"] = df.abbrev
    df.loc[missing, "error_msg"] = "bad abbrev"

    df_new = pd.DataFrame({
        "location": df["location"],
        "main_page": df["main_page"],
        "data_page": df["data_page"],
        "error_msg": df["error_msg"],
    })    
    return df_new

# ------------------------------------------
def parse_states(content: bytes) -> pd.DataFrame:

    sheet = GoogleSheet(content)
    df = sheet.get_tab("States")

    #print(f"columns = \n{df.columns}")
    df_new = pd.DataFrame({
        "location": df["State"],
        "main_page": df["COVID-19 site"].apply(clean_google_url),
        "data_page": df["COVID-19 site (secondary)"].apply(clean_google_url),
    })    
    return df_new

# ------------------------------------------
def parse_community_counties(content: bytes) -> pd.DataFrame:

    try:

        doc = html.fromstring(content)
        table = doc.find(".//table")


        # data the columns out of an HTML table
        num_cols = 14 # outcome
        names = ["row"]
        cols = [[]]

        def text(x: html.Element):
            t = x.text
            if t == None: t = ""
            if len(x) > 0:
                for ch in x:
                    t += " " + text(ch)
            if x.tail != None:
                t += " " + x.tail
            return t.strip()

        def extract(x: html.Element, row_num: int) -> int:
            if x.tag == "tr":
                row_num += 1
                if row_num == 1: return row_num

                if row_num > 2:
                    cols[0].append(row_num-2)

                i = 0
                for ch in x:   
                    t = text(ch)
                    if row_num == 2:
                        names.append(t)
                        cols.append([])
                    elif i < len(names):
                        cols[i+1].append(t)
                    if i == num_cols: break
                    i += 1
                return row_num
            elif x.tag in ["table", "thead", "tbody"]:
                for ch in x:
                    row_num = extract(ch, row_num)
                return row_num
            else:
                print(f"bad tag {x.tag}")             
        extract(table, 0)
        
        df = pd.DataFrame(index = cols[0])
        for i in range(len(names)):
            n = names[i]
            df[n] = cols[i]

        df = df[df.Country == "USA"]
        df = df[~pd.isnull(df["Abbr."])]
        df = df[df["Abbr."].str.strip() != ""]
        
        df_new = pd.DataFrame({
            "location": df["Abbr."],
            "main_page": df["Source"],
        })    
        return df_new

    except Exception as ex:
        logger.exception(ex)
        exit(-1)

# ------------------------------------------
def parse_cds(content: bytes) -> pd.DataFrame:
    
    recs = json.loads(content)
    df = pd.DataFrame(recs)
    df.reset_index(inplace=True)

    df = df[df.country == "USA"]
    df = df[~pd.isnull(df.county)]

    def clean_name(s: str) -> str:
        if s == None or s == "": return ""
        s = s.replace(" County", "")
        s = s.replace(". ", "_")
        s = s.replace(".", "_")
        s = s.replace(" ", "_")
        s = s.replace("'", "")
        s = s.replace(",", "_")
        return s

    df["location"] = df.state + "." + df.county.apply(clean_name)


    #TODO: add population as a comment


    df_new = pd.DataFrame({
        "location": df.location,
        "main_page": df.url
    })    
    return df_new


# ------------------------------------------
sources_config = [
    { 
        "name": "google-states-csv", 
        "subfolder": "",
        "endpoint": "https://covid.cape.io/states/info.csv", 
        "content_type": "csv",
        "parser": parse_google_csv,
        "action": "enabled",
        "display_dups": False
    },
    { 
        "name": "google-states", 
        "subfolder": "",
        "endpoint": "https://docs.google.com/spreadsheets/d/18oVRrHj3c183mHmq3m89_163yuYltLNlOmPerQ18E8w/htmlview?sle=true#",
        "parser": parse_states, 
        "content_type": "html",
        "action": "disabled",
        "display_dups": False
    },
    { 
        "name": "urlwatch", 
        "subfolder": "",
        "endpoint": "https://covidtracking.com/api/urls", 
        "content_type": "json",
        "parser": parse_urlwatch,
        "action": "enabled",
        "display_dups": False
    },
    { 
        "name": "cds", 
        "subfolder": "",
        "endpoint": "http://blog.lazd.net/coronadatascraper/data.json",
        "parser": parse_cds, 
        "content_type": "json",
        "action": "enabled",
        "display_dups": False
    },
    { 
        "name": "community-data-counties", 
        "subfolder": "counties",
        "endpoint": "https://docs.google.com/spreadsheets/d/1T2cSvWvUvurnOuNFj2AMPGLpuR2yVs3-jdd_urfWU4c/edit#gid=1477768381",
        "content_type": "html",
        "parser": parse_community_counties, 
        "action": "test",
        "display_dups": False
    }
]


