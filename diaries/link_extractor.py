from collections.abc import Iterable
from io import StringIO
import pandas as pd
import logging
from utils.s3 import S3
import os


logger = logging.getLogger(__name__)


class LinkExtractor():
    """
    Extracts a table from an HTML and stores the table as CSV.
    The HTML contains the developer diaries for Paradox Interactive games.
    """

    def __init__(self, start_url: str, tgt_path: str, destination: str, file_name: str = 'diaries.csv'):
        """
        Initializes the LinkExtractor with the given parameters.

        Args:
        - start_url: the URL to extract links from
        - tgt_path: the target path to save the CSV file to
        - destination: the destination where the CSV file will be saved; currently supported: s3 & local
        - file_name: the name of the CSV file (default is 'diaries.csv')
        """
        self.start_url = start_url
        self.tgt_path = tgt_path
        self.destination = destination
        self.file_name = file_name
        self.file_path = f'{self.tgt_path}/{self.file_name}'

    def _get_dev_diaries(self, match: str) -> pd.DataFrame:
        """
        Gets development diaries from the given URL and returns them as a pandas dataframe.

        Args:
        - match: a regex pattern to match the links to extract

        Returns:
        - df: a pandas dataframe containing the development diaries
        """
        try:
            tables = pd.read_html(self.start_url, header=0,
                                  extract_links='body', match=match)
        except Exception as error:
            logger.error('Unable to fetch table caused by %s', error)
        df = pd.concat(tables)
        return df

    @staticmethod
    def _reorder_columns(df: pd.DataFrame, idx: int, columns: list):
        """
        Reorders the columns in the given dataframe.

        Args:
        - df: a pandas dataframe
        - idx: the index to insert the new columns at
        - columns: a list of column names to insert
        """
        for i, column in enumerate(columns):
            values = df[column].values
            df.pop(column)
            df.insert(idx+i, column, values)

    def _remove_none(self, df: pd.DataFrame, column: str) -> list:
        """
        Removes None values from the given column in the dataframe and returns the cleaned values as a list.

        Args:
        - df: a pandas dataframe
        - column: the name of the column to clean

        Returns:
        - cleaned_values: a list of cleaned values from the given column
        """
        return [''.join(filter(None, (x[0], x[1]))) if isinstance(x, Iterable) else x for x in df[column].values]

    def _separate_tuple(self, df: pd.DataFrame, column: str, sep: str = 'and') -> None:
        """
        Separates the values in the given column that are tuples and creates new columns with the separated values.

        Args:
        - df: a pandas dataframe
        - column: the name of the column to separate
        - sep: the string to use to split the column name (default is 'and')

        Returns:
        - None
        """
        idx = df.columns.get_loc(column)
        new_column_names = [name.strip() for name in column.split(sep)]
        if len(new_column_names) != 2:
            logger.exception(
                'Number of columns %s does not equal requirement: 2', len(new_column_names))
        df[new_column_names[0]], df[new_column_names[1]] = zip(*df[column])

        df.drop(columns=column, inplace=True)
        self._reorder_columns(df, idx, new_column_names)

    def _clean_dataframe(self, df: pd.DataFrame) -> None:
        """
        Cleans the given dataframe by removing None values and separating tuple values into new columns.

        Args:
        - df: a pandas dataframe

        Returns:
        - None
        """
        for column in df.columns:
            if all(isinstance(value, Iterable) for value in df[column]):
                if not all(map(all, df[column].values)):
                    df[column] = self._remove_none(df, column)
                else:
                    self._separate_tuple(df, column)

    def _write_dataframe(self, df: pd.DataFrame) -> None:
        """
        Writes the given dataframe to the specified destination.

        Args:
        - df: a pandas dataframe

        Returns:
        - None
        """
        if self.destination == 's3':
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3_client = S3()
            s3_client.object_to_bucket(object=csv_buffer.getvalue(),
                                       file_path=self.file_path)
        else:
            data_path = f'./data'
            local_path = f'{data_path}/{self.file_path}'
            logger.info('Writing file to path %s', local_path)
            if not os.path.exists(f'{data_path}/{self.tgt_path}'):
                os.mkdir(data_path)
            df.to_csv(local_path, index=False)

    def main(self) -> None:
        """
        Extracts developer diaries from the given start URL, cleans the data, and writes it to the specified destination.

        Args:
        - None

        Returns:
        - df: a pandas dataframe
        """
        logger.info('Downloading developer diaries from %s', self.start_url)
        df = self._get_dev_diaries(match='Title and Link')
        df.to_csv('test2.csv')
        self._clean_dataframe(df)
        try:
            self._write_dataframe(df)
        except Exception as error:
            logger.exception('Unable to write file; Error: %s', error)
            raise
