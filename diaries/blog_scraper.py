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
    doc tag
    """

    def __init__(self, src_path, tgt_path, destination):
        self.src_path = src_path
        self.tgt_path = tgt_path
        self.destination = destination
        self._get_data()

    def _get_data(self):
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

    def _extract_blog(self, link):
        response = trafilatura.fetch_url(link)
        if response:
            blog = trafilatura.extract(response, favor_precision=True)
        else:
            logger.exception(
                'Failed to retrieve text from URL: "%s"', link)
        return blog

    def _write_txt(self, subject, blog, overwrite=False):
        file_path = f'./data/{self.tgt_path}/{subject}.txt'
        if os.path.exists(file_path) and not overwrite:
            logger.info('File %s exists; skipping overwrite', file_path)
        else:
            logger.info('Writing file: %s', file_path)
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(blog)

    def _write_to_s3(self, subject, blog):
        s3_client = S3()
        file_path = f'{self.tgt_path}/{subject}.txt'
        s3_client.object_to_bucket(object=blog,
                                   file_path=file_path)

    def _worker(self, title, link):
        blog = self._extract_blog(link)
        subject = re.sub('[^A-Za-z0-9]+', ' ', title).replace(' ', '_').lower()
        if self.destination == 's3':
            self._write_to_s3(subject, blog)
        else:
            self._write_txt(subject, blog)
        time.sleep(5)

    def main(self):
        # for title, link in zip(self.titles, self.links):
        #     self._worker(title, link)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            _ = [executor.submit(self._worker, title, link) for title, link in zip(self.titles, self.links)]
            # for title, link in zip(self.titles, self.links):
            #     _ = executor.submit(self._worker, title, link)


if __name__ == '__main__':
    blog = BlogScraper(src_path='./data/data.csv',
                       tgt_path='data', destination='local')
    blog.main()
