import trafilatura
import pandas as pd
import time
import os
import re
import logging
from utils.s3 import S3
from concurrent.futures import ThreadPoolExecutor


logging.getLogger('trafilatura').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
MAX_WORKERS = 3


class BlogScraper:
    """
    Scrapes developer diary blogs and stores in TXT files.
    The developer diary links are fetched from a pandas dataframe.
    """

    def __init__(self, src_path: str, tgt_path: str, destination: str):
        """
        Initializes the BlogScraper with the given parameters.

        Args:
        - src_path: the source path of CSV file containing diary links
        - tgt_path: the target path to save the TXT files to
        - destination: the destination where the CSV file will be saved; currently supported: s3 & local
        """
        self.src_path = src_path
        self.tgt_path = tgt_path
        self.destination = destination
        self._get_data()

    def _get_data(self) -> None:
        """
        Helper method to initialize the class. 
        Opens the src_path and reads blog titles and links.

        Args:
        - None

        Returns:
        - None
        """
        if self.destination == 's3':
            logger.info('Fetching %s...', self.src_path)
            s3_client = S3()
            response = s3_client.object_from_bucket(file_path=self.src_path)
            df = pd.read_csv(response)
        else:
            local_path = f'./data/{self.src_path}'
            df = pd.read_csv(local_path)

        df = df[~df['Link'].str.contains('youtube')]
        self.titles = df['Title'].values
        self.links = df['Link'].values

    def _extract_blog(self, link: str) -> str:
        """
        Extracts text from blog.

        Args:
        - link: The link of the blog

        Returns:
        - blog: The text of the blog
        """
        response = trafilatura.fetch_url(link)
        if response:
            blog = trafilatura.extract(response, favor_precision=True)
        else:
            logger.exception(
                'Failed to retrieve text from URL: "%s"', link)
        return blog

    def _write_txt(self, subject: str, blog: str, overwrite: bool = False) -> None:
        """
        Writes blog text to local destination.

        Args:
        - subject: The name of the TXT file
        - blog: The text of the blog
        - overwrite: Condition to overwrite file if exists

        Returns:
        - None
        """
        file_path = f'./data/{self.tgt_path}/{subject}.txt'
        if os.path.exists(file_path) and not overwrite:
            logger.info('File %s exists; skipping overwrite', file_path)
        else:
            logger.info('Writing file: %s', file_path)
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(blog)

    def _write_to_s3(self, subject: str, blog: str) -> None:
        """
        Writes blog text to S3 destination.

        Args:
        - subject: The name of the TXT file
        - blog: The text of the blog

        Returns:
        - None
        """
        s3_client = S3()
        file_path = f'{self.tgt_path}/{subject}.txt'
        s3_client.object_to_bucket(object=blog,
                                   file_path=file_path)

    def _worker(self, title, link) -> None:
        """
        Method that executes blog extraction and stores file at destination.
        Sleeps for 3 seconds after execution to avoid stressing PDX server.

        Args:
        - title: The developer diary title
        - link: The developer diary link

        Returns:
        - None
        """
        blog = self._extract_blog(link)
        subject = re.sub('[^A-Za-z0-9]+', ' ', title).replace(' ', '_').lower()
        if self.destination == 's3':
            self._write_to_s3(subject, blog)
        else:
            self._write_txt(subject, blog)
        time.sleep(3)

    def main(self) -> None:
        """
        Uses multithreading to execute _worker method in a loop for all blogs.

        Args:
        - None

        Returns:
        - None
        """
        # for title, link in zip(self.titles, self.links):
        #     self._worker(title, link)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            _ = [executor.submit(self._worker, title, link)
                 for title, link in zip(self.titles, self.links)]
