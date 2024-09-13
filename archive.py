import logging
import sys
from os import listdir, getcwd, chdir
from os.path import isfile, abspath

import boto3
import botocore

S3_SERVICE_NAME = 's3'
LOG_FORMAT = '%(levelname)s: %(asctime)s: %(message)s'
LOG_LEVEL = logging.INFO
MAX_POOL_CONNECTIONS = 100

s3 = boto3.client(S3_SERVICE_NAME, config=botocore.config.Config(
    max_pool_connections=MAX_POOL_CONNECTIONS,
))
s3_rsrc = boto3.resource(S3_SERVICE_NAME)

def bucket_exists(bucket_name):
    try:
        response = s3.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        logging.debug(e)
        return False
    return True

def get_paths(folder):
    initial_dir = getcwd()
    chdir(folder)
    onlyfiles = [(f, abspath(f)) for f in listdir() if isfile(f)]
    chdir(initial_dir)
    return onlyfiles

def put_file(file, bucket, key, prefix):
    s3.put_object(Body=open(file, 'rb'), ACL='private', Key=f'{prefix}/{key}', Bucket=bucket, StorageClass='GLACIER')

def main():
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    # ./archive.py <folder> <bucket> <prefix>
    folder = sys.argv[1]
    bucket = sys.argv[2]
    prefix = ''
    if len(sys.argv) > 3:
        prefix = sys.argv[3]

    if not bucket_exists(bucket):
        logging.error(f'''Bucket {bucket} doesn't exist''')
        return 1

    logging.info('Populating list of objects...')

    files = get_paths(folder)
    for (filename, path) in files:
        put_file(path, bucket, filename, prefix)
        logging.info(f'File {filename} uploaded')

    return 0


if __name__ == '__main__':
   sys.exit(main())
