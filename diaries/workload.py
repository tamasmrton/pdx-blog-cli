import requests
import questionary
from bs4 import BeautifulSoup
from .link_extractor import LinkExtractor
from .blog_scraper import BlogScraper
from func_timeout import func_timeout
import logging
import logging.config
import re


logging.getLogger('requests').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class Workload:
    def __init__(self,
                 selected_game: str = None,
                 url: str = 'https://wikis.paradoxplaza.com/',
                 destination: str = 'local'):
        self.url = url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
        self.selected_game = selected_game
        self.destination = destination

    def _fetch_wiki(self):
        try:
            logger.info('Fetching %s...', self.url)
            result = requests.get(
                self.url, headers=self.headers, timeout=60)
        except Exception as error:
            logger.exception('Unable to fetch url; Error: %s', error)
            raise
        soup = BeautifulSoup(result.content, 'lxml')
        wikis = {}
        for li_tag in soup.find_all('li'):
            p_tag = li_tag.find('p').string
            href = li_tag.find('a', href=True).get("href")
            wikis[p_tag] = href
        return wikis

    def _fetch_diary(self, wiki_link) -> str:
        result = requests.get(wiki_link, headers=self.headers, timeout=60)
        soup = BeautifulSoup(result.content, 'lxml')

        try:
            relative_path = soup.find(
                'a', {'title': 'Developer diaries'}).get('href')
        except AttributeError as error:
            logger.exception(
                'Unable to retrieve developer diary; Error: %s', error)
            raise
        absolute_path = wiki_link + relative_path
        return absolute_path

    @staticmethod
    def _select_game(games: list) -> str:
        selected_game = questionary.select(
            "Which game's developer diary do you want?", choices=games).ask()
        return selected_game

    def main(self):
        wikis = self._fetch_wiki()
        selectable_games = list(wikis.keys())

        if not self.selected_game:
            self.selected_game = func_timeout(
                10, self._select_game, args=(selectable_games,))

        try:
            selected_url = wikis[self.selected_game]
        except KeyError as error:
            logger.exception(
                'The game name passed is incorrect; Error: %s', error)
            raise

        diary = self._fetch_diary(selected_url)
        subject = re.sub('[^A-Za-z0-9]+', ' ',
                         self.selected_game).replace(' ', '_').lower()
        links = LinkExtractor(
            start_url=diary, tgt_path=subject, destination=self.destination)
        links.main()
        blog = BlogScraper(
            src_path=links.file_path, tgt_path=subject, destination=self.destination)
        blog.main()


if __name__ == '__main__':
    load = Workload()
    load.main()
