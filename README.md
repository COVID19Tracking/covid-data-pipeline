# corona19-data-pipeline

Scan/Trim/Extract Pipeline for Coronavirus Site

## Scanner

1. Gets the data from urls in google sheet.
2. Pulls the raw HTML
3. Creates a clean version without the markup
4. Push it into a github repo.

## Specialized_Capture

1. Fire up a captive browser
2. For a list of urls, take a screen shot
3. If they change, push them into git


## Pages that don't fetch data currently

AZ uses tableau.  page never changes as a result.
WA, ND, and WV all use a javascript redirect.
IA uses a recaptura.
