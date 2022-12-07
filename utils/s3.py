import logging
import boto3
from botocore.exceptions import ClientError
import os

logger = logging.getLogger(__name__)


class S3():

    def __init__(self):
        self.access_key = os.environ['AWS_ACCESS_KEY']
        self.secret_key = os.environ['AWS_SECRET_KEY']
        self.bucket = os.environ['AWS_BUCKET']
        self.create_session()

    def create_session(self):
        session = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key)
        self.s3_client = session.client('s3')

    def file_to_bucket(self, file_path, object_name):
        if object_name is None:
            object_name = os.path.basename(file_path)

        # Upload the file
        try:
            self.s3_client.upload_file(file_path, self.bucket, object_name)
        except Exception as error:
            logger.error('Unable to upload file to bucket; Error: %s', error)

    def object_to_bucket(self, object, file_path):
        try:
            self.s3_client.put_object(Body=object,
                                      Bucket=self.bucket,
                                      Key=file_path)
            logger.info('File %s uploaded to %s', file_path, self.bucket)
        except Exception as error:
            logger.error('Unable to upload object to bucket; Error: %s', error)

    def object_from_bucket(self, file_path):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket,
                                                 Key=file_path)
        except Exception as error:
            logger.error(
                'Unable to fetch object from bucket; Error: %s', error)
        return response.get('Body')
