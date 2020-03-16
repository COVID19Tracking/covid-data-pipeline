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

from captive_browser import CaptiveBrowser
from url_source import UrlSource, get_available_sources

parser = ArgumentParser(
    description=__doc__,
    formatter_class=RawDescriptionHelpFormatter)

parser.add_argument(
    '--temp-dir',
    default='/tmp/public-cache',
    help='Local temp dir for snapshots')

parser.add_argument(
    '--s3-bucket',
    default='covid-data-archive',
    help='S3 bucket name')

parser.add_argument('--push-to-s3', dest='push_to_s3', action='store_true', default=False,
    help='push screenshots to S3')


class S3Backup():

    def __init__(self, bucket: str):
        self.s3 = boto3.resource('s3')
        self.bucket = bucket

    # for now just uploads image (PNG) file with specified name
    def upload_file(self, local_path: str, s3_path: str):
        self.s3.meta.client.upload_file(
            local_path, self.bucket, s3_path,
            ExtraArgs={'ContentType': 'image/png'})


def main(args_list=None):
    if args_list is None:
        args_list = sys.argv[1:]
    args = parser.parse_args(args_list)

    browser = CaptiveBrowser()
    s3 = S3Backup(bucket=args.s3_bucket)

    # get states info from API
    url_sources = get_available_sources()
    logger.info(f"processing source {url_sources[0].name}")
    state_info_df = url_sources[0].load()

    for idx, r in state_info_df.iterrows():
        # if idx > 1:
            # break
        state = r["state"]
        data_url = r["data_page"]

        logger.info(f"Screenshotting {state} from {data_url}")

        timestamp = datetime.now(timezone('US/Eastern')).strftime("%Y%m%d-%H%M%S")
        filename =  "%s-%s.png" % (state, timestamp)
        local_path = os.path.join(args.temp_dir, filename)

        logger.info(f"    1. get content from {data_url}")
        browser.get(data_url)
        
        logger.info(f"    2. wait for 5 seconds")
        time.sleep(5)

        logger.info(f"    3. save screenshot to {local_path}")
        browser.screenshot(local_path)

        if args.push_to_s3:
            s3_path = os.path.join('state_screenshots', state, filename)
            logger.info(f"    4. push to s3 at {s3_path}")
            s3.upload_file(local_path, s3_path)


if __name__ == "__main__":
    main()
