from collections.abc import Iterable
from io import StringIO
import pandas as pd
import logging
from utils.s3 import S3
import os


logger = logging.getLogger(__name__)


class LinkExtractor():
    """
    doc tag
    """

    def __init__(self, start_url: str, tgt_path: str, destination: str, file_name: str = 'diaries.csv'):
        self.start_url = start_url
        self.tgt_path = tgt_path
        self.destination = destination
        self.file_name = file_name
        self.file_path = f'{self.tgt_path}/{self.file_name}'

    def _get_dev_diaries(self, match):
        try:
            tables = pd.read_html(self.start_url, header=0,
                                  extract_links='body', match=match)
        except Exception as error:
            logger.error('Unable to fetch table caused by %s', error)
        df = pd.concat(tables)
        return df

    @staticmethod
    def _reorder_columns(df: pd.DataFrame, idx: int, columns: list):
        for i, column in enumerate(columns):
            values = df[column].values
            df.pop(column)
            df.insert(idx+i, column, values)

    def _remove_none(self, df: pd.DataFrame, column):
        print(df[column].head(5))
        return [''.join(filter(None, (x, y))) for x, y in df[column].values]

    def _separate_tuple(self, df: pd.DataFrame, column: str, sep: str = 'and'):
        idx = df.columns.get_loc(column)
        new_column_names = [name.strip() for name in column.split(sep)]
        if len(new_column_names) != 2:
            logger.exception(
                'Number of columns %s does not equal requirement: 2', len(new_column_names))
        df[new_column_names[0]], df[new_column_names[1]] = zip(*df[column])

        df.drop(columns=column, inplace=True)
        self._reorder_columns(df, idx, new_column_names)

    def _clean_dataframe(self, df: pd.DataFrame):
        for column in df.columns:
            if isinstance(df[column].str, Iterable):
                if not all(map(all, df[column].values)):
                    df[column] = self._remove_none(df, column)
                else:
                    self._separate_tuple(df, column)

    def _write_dataframe(self, df: pd.DataFrame):
        if self.destination == 's3':
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3_client = S3()
            s3_client.object_to_bucket(object=csv_buffer.getvalue(),
                                       file_path=self.file_path)
        else:
            data_path = './data'
            local_path = f'{data_path}/{self.file_path}'
            logger.info('Writing file to path %s', local_path)
            if not os.path.exists(data_path):
                os.mkdir(data_path)
            df.to_csv(local_path, index=False)

    def main(self) -> pd.DataFrame:
        logger.info('Downloading developer diaries from %s', self.start_url)
        df = self._get_dev_diaries(match='Title and Link')
        self._clean_dataframe(df)
        try:
            self._write_dataframe(df)
        except Exception as error:
            logger.exception('Unable to write file; Error: %s', error)
            raise
