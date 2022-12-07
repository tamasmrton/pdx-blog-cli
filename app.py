import sys
from diaries.workload import Workload
import logging
from time import perf_counter
import click

logging.basicConfig(stream=sys.stdout, format='%(asctime)s %(name)s: %(levelname)-4s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option('--game', default=None, type=str, help='The selected Paradox game.')
@click.option('--destination', default='local', type=click.Choice(['local', 's3']),
              help='Select the download destination.')
def worker(game: str, destination: str = 'local'):
    """
    Paradox game developer diary scraper.
    """
    workload = Workload(selected_game=game,
                        destination=destination)
    workload.main()


if __name__ == '__main__':
    t1_start = perf_counter()
    worker()
    t1_stop = perf_counter()
    t_diff = t1_stop-t1_start
    logger.info("Elapsed time during the whole program in seconds: %s", t_diff)
