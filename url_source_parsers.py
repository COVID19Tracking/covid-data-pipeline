# various url source parsers
import io
import pandas as pd
import urllib.parse
import json
from loguru import logger

from google_sheet import GoogleSheet

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
    #print(f"df = \n{df}")

    try:
        df["location"] = df["state"]
        df["main_page"] = df["covid19Site"].apply(clean_google_url) 
        df["data_page"] = df["dataSite"].apply(clean_google_url) 
    except Exception as ex:
        logger.error(ex)
        logger.info(f"df = \n{df}")
        raise Exception("can't parse google CSV")

    return df

# ------------------------------------------
def parse_urlwatch(content: bytes) -> pd.DataFrame:
    
    recs = json.loads(content)
    df = pd.DataFrame(recs)
    df["location"] = df["name"]
    df["main_page"] = df["url"].apply(clean_google_url) 
    df["data_page"] = ""

    names = {}
    for x in df.itertuples():
        cnt = names.get(x.name)
        if cnt == None: cnt = 0
        names[x.name] = cnt + 1

        if cnt == 1:
            print(f"assign 2nd url for {x.name} to data")
            df.iloc[x.Index, "data_page"] = x.main_page
            df.iloc[x.Index, "main_page"] = ""
            df.iloc[x.Index, "location"] += "_data"
            print(df.iloc[x.Index])
        elif cnt > 1:            
            print(f"scanner does not support {cnt} urls for {x.name} -> ignore")
            df.iloc[x.Index, "main_page"] = ""
            df.iloc[x.Index, "location"] += f"_{cnt}"

    print(f"columns = \n{df.columns}")
    return df

# ------------------------------------------
def parse_states(content: bytes) -> pd.DataFrame:

    sheet = GoogleSheet(content)
    df = sheet.get_tab("States")

    print(f"columns = \n{df.columns}")
    df_new = pd.DataFrame({
        "location": df["State"],
        "main_page": df["COVID-19 site"],
        "data_page": df["Data site"],
    })    
    return df_new

# ------------------------------------------
def parse_community_counties(content: bytes) -> pd.DataFrame:

    sheet = GoogleSheet(content)
    df = sheet.get_tab("USA County Sources")

    print(f"columns = \n{df.columns}")
    return df

# ------------------------------------------
def parse_cds(content: bytes) -> pd.DataFrame:
    
    recs = json.loads(content)
    df = pd.DataFrame(recs)

    print(f"columns = \n{df.columns}")
    return df


# ------------------------------------------
sources_config = [
    { 
        "name": "google-states", 
        "endpoint": "https://docs.google.com/spreadsheets/d/18oVRrHj3c183mHmq3m89_163yuYltLNlOmPerQ18E8w/htmlview?sle=true#",
        "parser": parse_states, 
        "content_type": "html",
        "display_dups": False
    },
    { 
        "name": "google-states-csv", 
        "endpoint": "https://covid.cape.io/states/info.csv", 
        "content_type": "csv",
        "parser": parse_google_csv,
        "display_dups": False
    },
    { 
        "name": "urlwatch", 
        "endpoint": "https://covidtracking.com/api/urls", 
        "content_type": "json",
        "parser": parse_urlwatch,
        "display_dups": False
    },
    { 
        "name": "community-data-counties", 
        "endpoint": "https://docs.google.com/spreadsheets/d/1T2cSvWvUvurnOuNFj2AMPGLpuR2yVs3-jdd_urfWU4c/edit#gid=1477768381",
        "content_type": "html",
        "parser": parse_community_counties, 
        "display_dups": False
    },
    { 
        "name": "cds", 
        "endpoint": "http://blog.lazd.net/coronadatascraper/#data.json",
        "parser": parse_cds, 
        "content_type": "json",
        "display_dups": False
    }    
]


