"""Script to run image capture screenshots for state data pages.
"""

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from datetime import datetime
import io
import os
from pytz import timezone
import sys
import time

import boto3
from loguru import logger

# change the the imports will work rather than failing mysteriously
from __init__ import check_path
check_path() 

from capture.captive_browser import CaptiveBrowser
from sources.url_source import load_one_source

from shared.util import read_config_file
config = read_config_file()

parser = ArgumentParser(
    description=__doc__,
    formatter_class=RawDescriptionHelpFormatter)

parser.add_argument(
    '--temp-dir',
    default=config["DIRS"]["test_dir"],
    #default='/tmp/public-cache',
    help='Local temp dir for snapshots')

parser.add_argument(
    '--s3-bucket',
    #default='covid-data-archive',
    default=config["S3"]["bucket_name"],
    help='S3 bucket name')

parser.add_argument('--states',
    default='',
    help='Comma-separated list of state 2-letter names. If present, will only screenshot those.')

parser.add_argument('--public-only', action='store_true', default=False,
    help='If present, will only snapshot public website and not state pages')

parser.add_argument('--push-to-s3', dest='push_to_s3', action='store_true', default=False,
    help='Push screenshots to S3')

parser.add_argument('--replace-most-recent-snapshot', action='store_true', default=False,
    help='If present, will first delete the most recent snapshot for the state before saving ' 
         'new screenshot to S3')


_PUBLIC_STATE_URL = 'https://covidtracking.com/data/'
_STATES_LARGER_WINDOWS = ['DE', 'IN', 'MA', 'NC', 'OK']

from .shared.udatetime import now_as_utc
from .shared.util import convert_python_to_json
import json

class S3Log():
    def __init__(self):
        self.items = []

    def record(self, name: str, url: str, status: str):
        self.items.append({
            "name": name,
            "url": url,
            "status": status,
            "at": now_as_utc(),
        })

    def save(self, path: str):
        convert_python_to_json(self.items)
        json.dump(path, self.items)

class S3Backup():

    def __init__(self, bucket_name: str):
        # pylint: disable=no-member
        self.s3 = boto3.resource('s3')
        self.bucket_name = bucket_name
        self.bucket = self.s3.Bucket(self.bucket_name)

    # for now just uploads image (PNG) file with specified name
    def upload_file(self, local_path: str, s3_path: str):
        self.s3.meta.client.upload_file(
            local_path, self.bucket_name, s3_path,
            ExtraArgs={'ContentType': 'image/png'})

    def delete_most_recent_snapshot(self, state: str):
        # pylint: disable=no-member
        state_file_keys = [file.key for file in self.bucket.objects.all() if state in file.key]
        most_recent_state_key = sorted(state_file_keys, reverse=True)[0]
        self.s3.Object(self.bucket_name, most_recent_state_key).delete()


# saves screenshot of data_url to specified path. takes 5 sec. can throw exception on load fail
def screenshot_to_path(data_url, path, browser):
    logger.info(f"    1. get content from {data_url}")
    if not browser.navigate(data_url):
        logger.error("  get timed out -> skip")
        return

    logger.info(f"    2. wait for 5 seconds")
    time.sleep(5)

    logger.info(f"    3. save screenshot to {path}")
    browser.screenshot(path, full_page=True)


def screenshot(state, data_url, args, s3, browser):
    logger.info(f"Screenshotting {state} from {data_url}")

    timestamp = datetime.now(timezone('US/Eastern')).strftime("%Y%m%d-%H%M%S")
    filename =  "%s-%s.png" % (state, timestamp)
    local_path = os.path.join(args.temp_dir, filename)

    try:
        screenshot_to_path(data_url, local_path, browser)
    except Exception as exc:
        logger.error(f"    Failed to screenshot {state}!")
        raise exc

    if args.push_to_s3:
        s3_path = os.path.join('state_screenshots', state, filename)
        if args.replace_most_recent_snapshot:
            logger.info(f"    3a. first delete the most recent snapshot")
            s3.delete_most_recent_snapshot(state)

        logger.info(f"    4. push to s3 at {s3_path}")
        s3.upload_file(local_path, s3_path)


def main(args_list=None):
    if args_list is None:
        args_list = sys.argv[1:]
    args = parser.parse_args(args_list)

    browser = CaptiveBrowser()
    s3 = S3Backup(bucket_name=args.s3_bucket)

    # get states info from API
    src = load_one_source("google-states-csv")
    state_info_df = src.df
    
    failed_states = []

    def screenshot_with_size_handling(state, data_url):
        # hack: if state needs to be bigger, capture that too. public site is huge.
        current_size = browser.driver.get_window_size()
        if state in _STATES_LARGER_WINDOWS:
            logger.info("temporarily resize browser to capture longer state pages")
            browser.driver.set_window_size(current_size["width"], current_size["height"] * 2)
        elif state == 'public':
            logger.info("temporarily resize browser to capture longer public page")
            browser.driver.set_window_size(current_size["width"], current_size["height"] * 4)

        try:
            screenshot(state, data_url, args, s3, browser)
        except Exception:
            failed_states.append(state)
        finally:
            logger.info("reset browser to original size")
            browser.driver.set_window_size(current_size["width"], current_size["height"])

    # screenshot public state site
    screenshot_with_size_handling('public', _PUBLIC_STATE_URL)

    if args.public_only:
        logger.info("Not snapshotting state pages, was asked for --public-only")
        return

    # screenshot state images
    if args.states:
        states = args.states.split(',')
        for state in states:
            data_url = state_info_df.loc[state_info_df.state == state].head(1).data_page.values[0]
            screenshot_with_size_handling(state, data_url)

    else:
        log = S3Log()

        for _, r in state_info_df.iterrows():
            # if idx > 1:
                # break
            state = r["location"]
            data_url = r["data_page"]
            screenshot_with_size_handling(state, data_url)

            log.record(state, data_url, "ok")

        log.save("log.json")

    if failed_states:
        logger.error(f"Failed states for this run: {','.join(failed_states)}")


if __name__ == "__main__":
    main()
