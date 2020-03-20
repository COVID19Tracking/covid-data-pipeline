import re

xlist = [
    "modebar-44d90d",
    "arc-News Archives-64",
    "pl-w5e726ccf05961",
    "b1d1a1-mapbox",
    "7brnlw-accordion-label",
    "overlay-16065"
]

#              <a href="https://www.iowa.gov?ia_slv=1584679309087">Iowa.gov</a>

for xid in xlist:
    xid2 = re.sub("^[0-9a-fA-F]+-(.*)",  "\\1", xid) 
    xid2 = re.sub("(.*)-[a-z]?[0-9a-fA-F]+$",  "\\1", xid2) 
    print(f"  {xid} -> {xid2}")

