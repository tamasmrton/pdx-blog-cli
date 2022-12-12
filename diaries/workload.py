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
    """
    The Workload class is responsible for fetching the developer diary for a selected game.
    It Orchestrates LinkExtractor and BlogScraper classes.
    """

    def __init__(self,
                 selected_game: str,
                 url: str = 'https://wikis.paradoxplaza.com/',
                 destination: str = 'local'):
        """
        Initializes the Workload with the given parameters.

        Args:
        - selected_game: the selected Paradox Interactive game
        - url: Source of list of games
        - destination: the destination where the CSV file will be saved; currently supported: s3 & local
        """
        self.url = url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
        self.selected_game = selected_game
        self.destination = destination

    def _fetch_wiki(self) -> dict:
        """
        Reads the list of games from url (wiki page of all Paradox Interactive games).

        Args:
        - None

        Returns:
        - wikis: dictionary of game and link pairs 
        """
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
        """
        Extracts the url of the Developer diary from a wiki link.

        Args:
        - wiki_link: The link of the game's wiki landing page

        Returns:
        - absolute_path: The absolute URL path of the Developer Diary.
        """
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
        """
        Prompt to choose a game from list of games. Used when --game tag is null.

        Args:
        - games: list of Pardox gamex to choose from

        Returns:
        - selected_game: str of the game chosen
        """
        selected_game = questionary.select(
            "Which game's developer diary do you want?", choices=games).ask()
        return selected_game

    def main(self) -> None:
        """
        Executes the tasks handled by Workload class.
        Namely, it fetches the main wiki page of Paradox & based on the selected game it finds the absolute path of the developer diary.
        Then, it executes LinkExtractor() and BlogScraper().

        Args:
        - None

        Returns:
        - None
        """
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
