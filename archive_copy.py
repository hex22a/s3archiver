import logging
import sys
import time
from functools import partial
from multiprocessing.pool import ThreadPool

import boto3
import botocore

S3_SERVICE_NAME = 's3'
LOG_FORMAT = '%(levelname)s: %(asctime)s: %(message)s'
MAX_POOL_CONNECTIONS = 100
NUMBER_OF_COPY_THREADS = 10
SOURCE_ARCHIVE_PROFILE_NAME = 'source_archive_profile'
RESTORE_DAYS = 7
POLLING_INTERVAL_SECONDS = 300
TIER = 'Standard'
LOG_LEVEL = logging.INFO


def bucket_exists(s3_bucket, bucket_name):
    try:
        response = s3_bucket.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        logging.debug(e)
        return False
    return True


def get_s3objects(source_client, bucket):
    paginator = source_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)

    s3objects = []
    for page in pages:
        s3objects.extend(page['Contents'])

    return s3objects


def count_remaining_and_request_restores(s3_rsrc, bucket, s3objects):
    restored_count = 0
    pending_count = 0
    just_requested_count = 0
    not_glacier_count = 0
    logging.info('Checking storage class for the requested objects')
    logging.info('Objects in Glacier or Deep Archive need to be restored before they can be copied')
    logging.info('Legend: 📦 ready to copy    📼 requesting restore    📤 restoring    ✅ restored')
    for s3object in s3objects:
        obj = s3_rsrc.Object(bucket, s3object['Key'])
        if obj.storage_class == 'GLACIER' or obj.storage_class == 'DEEP_ARCHIVE':
            if obj.restore is None:
                obj.restore_object(RestoreRequest={'Days': RESTORE_DAYS, 'GlacierJobParameters': {'Tier': TIER}})
                just_requested_count = just_requested_count + 1
                print('📼', end='')
                sys.stdout.flush()
            elif 'ongoing-request="true"' in obj.restore:
                print('📤', end='')
                sys.stdout.flush()
                pending_count = pending_count + 1
            elif 'ongoing-request="false"' in obj.restore:
                print('✅', end='')
                sys.stdout.flush()
                restored_count = restored_count + 1
        else:
            print('📦', end='')
            sys.stdout.flush()
            not_glacier_count = not_glacier_count + 1
    print('')
    logging.info('📦 Ready to copy:      ' + str(not_glacier_count))
    logging.info('📼 Requesting restore: ' + str(just_requested_count))
    logging.info('📤 Restoring:          ' + str(pending_count))
    logging.info('✅ Restored:           ' + str(restored_count))
    return pending_count + just_requested_count


def copy_s3object(source_client, source_bucket, dest_bucket, s3object):
    sys.stdout.flush()
    source_client.copy(CopySource={'Bucket': source_bucket, 'Key': s3object['Key']}, Bucket=dest_bucket,
                            Key=s3object['Key'])
    print('📥', end='')


def copy_s3objects(source_client, source_bucket, dest_bucket, s3objects):
    pool = ThreadPool(processes=NUMBER_OF_COPY_THREADS)
    pool.map(partial(copy_s3object, source_client, source_bucket, dest_bucket), s3objects)
    print('')


def main():
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    source_bucket = sys.argv[1]
    destination_bucket = sys.argv[2]

    botocore_config = botocore.config.Config(max_pool_connections=MAX_POOL_CONNECTIONS)
    destination_client = boto3.client(S3_SERVICE_NAME, config=botocore_config)

    source_session = boto3.Session(profile_name=SOURCE_ARCHIVE_PROFILE_NAME)
    source_client = source_session.client(S3_SERVICE_NAME, config=botocore_config)
    s3_rsrc = source_session.resource(S3_SERVICE_NAME)

    if not bucket_exists(source_client, source_bucket):
        logging.error(f'''Bucket {source_bucket} doesn't exist''')
        return 1

    if not bucket_exists(destination_client, destination_bucket):
        logging.error(f'''Bucket {destination_bucket} doesn't exist''')
        return 1

    logging.info('Populating list of objects...')

    s3objects = get_s3objects(source_client, source_bucket)

    while count_remaining_and_request_restores(s3_rsrc, source_bucket, s3objects) >= 1:
        logging.info(f'😴 Sleeping {POLLING_INTERVAL_SECONDS} seconds')
        time.sleep(POLLING_INTERVAL_SECONDS)

    logging.info('Copying objects from ' + source_bucket + ' to ' + destination_bucket)
    copy_s3objects(source_client, source_bucket, destination_bucket, s3objects)
    logging.info('Objects copied.')
    logging.info('Restoration complete.')

    return 0


if __name__ == '__main__':
    sys.exit(main())
