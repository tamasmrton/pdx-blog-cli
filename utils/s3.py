import logging
import boto3
from botocore.exceptions import ClientError
import os

logger = logging.getLogger(__name__)


class S3():
    """
    Class for interacting with Amazon S3 bucket.

    Args:
    - access_key: AWS access key.
    - secret_key: AWS secret key.
    - bucket: AWS S3 bucket name.
    """

    def __init__(self):
        """
        Initializes S3 session with access and secret keys, and bucket name.
        """
        self.access_key = os.environ['AWS_ACCESS_KEY']
        self.secret_key = os.environ['AWS_SECRET_KEY']
        self.bucket = os.environ['AWS_BUCKET']
        self.create_session()

    def create_session(self):
        """
        Creates an AWS session using the access and secret keys.
        """
        session = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key)
        self.s3_client = session.client('s3')

    def file_to_bucket(self, file_path, object_name):
        """
        Uploads a file to the S3 bucket.

        Args:
        - file_path: Path where the file will be uploaded
        - object_name: Name to be given to the file in the bucket. If not provided, uses the file's original name.
        """
        if object_name is None:
            object_name = os.path.basename(file_path)

        # Upload the file
        try:
            self.s3_client.upload_file(file_path, self.bucket, object_name)
        except Exception as error:
            logger.error('Unable to upload file to bucket; Error: %s', error)

    def object_to_bucket(self, object, file_path):
        """
        Uploads an object to the S3 bucket.

        Args:
        - object: The name of the object
        - file_path: Path where the file will be uploaded
        """
        try:
            self.s3_client.put_object(Body=object,
                                      Bucket=self.bucket,
                                      Key=file_path)
            logger.info('File %s uploaded to %s', file_path, self.bucket)
        except Exception as error:
            logger.error('Unable to upload object to bucket; Error: %s', error)

    def object_from_bucket(self, file_path):
        """
        Retrieve an object from the S3 bucket.

        Args:
        - file_path: Path where the file is located
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket,
                                                 Key=file_path)
            body = response.get('Body')
            return body
        except Exception as error:
            logger.error(
                'Unable to fetch object from bucket; Error: %s', error)
