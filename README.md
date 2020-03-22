# corona19-data-pipeline

Scan/Trim/Extract Pipeline for Coronavirus Site

> * The code now expects to be run from the root directory of the repo. *
> * This includes IDEs like VS Code.  * 

---

## Scanner

1. Gets the data from urls in google sheet.
2. Pulls the raw HTML
3. Creates a clean version without the markup
4. Push it into a github repo.

## Backup To S3

1. pulls an image for each page
2. pushed it to an S3 bucket


## Specialized_Capture

1. Fire up a captive browser
2. For a list of urls, take a screen shot
3. If they change, push them into git


