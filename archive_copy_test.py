import unittest
import sys
from argparse import Namespace
from unittest.mock import patch, call

import archive_copy


@patch('archive_copy.ArgumentParser')
@patch('boto3.Session')
@patch('boto3.client')
@patch('botocore.config.Config')
@patch('logging.info')
@patch('logging.error')
@patch('logging.basicConfig')
@patch('archive_copy.bucket_exists', return_value=True)
@patch('archive_copy.copy_s3objects')
@patch('archive_copy.count_remaining_and_request_restores', return_value=0)
class TestArchiveCopy(unittest.TestCase):
    def test_main(
            self,
            mock_count_remaining_and_request_restores,
            mock_copy_s3objects,
            mock_bucket_exists,
            mock_logging_basic_config,
            mock_logging_error,
            mock_logging_info,
            mock_botocore_config_constructor,
            mock_boto3_client,
            mock_boto3_session,
            mock_argument_parser
    ):
        # Arrange
        expected_description = 'A tool that helps organizing S3 archives'
        expected_epilogue = 'Have a nice day!'
        expected_positional_argument_source_bucket = 'source_bucket'
        expected_positional_argument_destination_bucket = 'destination_bucket'
        expected_fast_access_flag_short = '-f'
        expected_fast_access_flag_full = '--fast-access'
        expected_fast_access_flag_help = "don't use StorageClass=GLACIER, instead use default StorageClass. Read more about Amazon S3 Storage Classes: https://aws.amazon.com/s3/storage-classes/"

        expected_s3_objects = ['file1.csv', 'file2.csv']
        expected_source_bucket = 'my-old-archives'
        expected_destination_bucket = 'my-new-archives'
        expected_args = Namespace(source_bucket=expected_source_bucket, destination_bucket=expected_destination_bucket, fast_access=False)
        expected_exit_code = 0
        expected_log_format = archive_copy.LOG_FORMAT
        expected_s3_service_name = archive_copy.S3_SERVICE_NAME
        expected_profile_name = archive_copy.SOURCE_ARCHIVE_PROFILE_NAME
        expected_max_pool_connections = archive_copy.MAX_POOL_CONNECTIONS
        expected_copy_to_glacier = True

        mock_parser = mock_argument_parser.return_value
        mock_source_session = mock_boto3_session.return_value
        mock_source_client = mock_source_session.client.return_value
        mock_s3_rsrc = mock_source_session.resource.return_value
        mock_botocore_config = mock_botocore_config_constructor.return_value
        mock_destination_client = mock_boto3_client.Client.return_value
        mock_parser.parse_args.return_value = expected_args

        expected_add_argument_calls = [
            call(expected_positional_argument_source_bucket),
            call(expected_positional_argument_destination_bucket),
            call(expected_fast_access_flag_short, expected_fast_access_flag_full, action='store_true', help=expected_fast_access_flag_help),
        ]

        expected_bucket_exists_calls = [
            call(mock_source_client, expected_source_bucket),
            call(mock_destination_client, expected_destination_bucket),
        ]

        with patch('archive_copy.get_s3objects', return_value=expected_s3_objects) as mock_get_s3objects:
            # Act
            actual_exit_code = archive_copy.main()

            # Assert
            self.assertEqual(actual_exit_code, expected_exit_code)
            mock_argument_parser.assert_called_with(description=expected_description, epilog=expected_epilogue)
            mock_parser.add_argument.assert_has_calls(expected_add_argument_calls)
            mock_parser.print_usage.assert_called()
            mock_parser.parse_args.assert_called()
            mock_logging_basic_config.assert_called_with(level=archive_copy.LOG_LEVEL, format=expected_log_format)
            mock_botocore_config_constructor.assert_called_with(max_pool_connections=expected_max_pool_connections)
            mock_boto3_client.assert_called_with(expected_s3_service_name, config=mock_botocore_config)
            mock_boto3_session.assert_called_with(profile_name=expected_profile_name)
            mock_source_session.client.assert_called_with(expected_s3_service_name, config=mock_botocore_config)
            mock_bucket_exists.asset_has_calls(expected_bucket_exists_calls)
            mock_get_s3objects.assert_called_with(mock_source_client, expected_source_bucket)
            mock_count_remaining_and_request_restores.assert_called_with(
                mock_s3_rsrc,
                expected_source_bucket,
                expected_s3_objects
            )
            mock_copy_s3objects.assert_called_with(
                mock_source_client,
                expected_source_bucket,
                expected_destination_bucket,
                expected_s3_objects,
                expected_copy_to_glacier
            )

    def test_main_fast_access(
            self,
            mock_count_remaining_and_request_restores,
            mock_copy_s3objects,
            mock_bucket_exists,
            mock_logging_basic_config,
            mock_logging_error,
            mock_logging_info,
            mock_botocore_config_constructor,
            mock_boto3_client,
            mock_boto3_session,
            mock_argument_parser
    ):
        # Arrange
        expected_description = 'A tool that helps organizing S3 archives'
        expected_epilogue = 'Have a nice day!'
        expected_positional_argument_source_bucket = 'source_bucket'
        expected_positional_argument_destination_bucket = 'destination_bucket'
        expected_fast_access_flag_short = '-f'
        expected_fast_access_flag_full = '--fast-access'
        expected_fast_access_flag_help = "don't use StorageClass=GLACIER, instead use default StorageClass. Read more about Amazon S3 Storage Classes: https://aws.amazon.com/s3/storage-classes/"
        expected_s3_objects = ['file1.csv', 'file2.csv']
        expected_source_bucket = 'my-old-archives'
        expected_destination_bucket = 'my-new-archives'
        expected_args = Namespace(source_bucket=expected_source_bucket, destination_bucket=expected_destination_bucket, fast_access=True)
        expected_exit_code = 0
        expected_log_format = archive_copy.LOG_FORMAT
        expected_s3_service_name = archive_copy.S3_SERVICE_NAME
        expected_profile_name = archive_copy.SOURCE_ARCHIVE_PROFILE_NAME
        expected_max_pool_connections = archive_copy.MAX_POOL_CONNECTIONS
        expected_copy_to_glacier = False

        mock_parser = mock_argument_parser.return_value
        mock_source_session = mock_boto3_session.return_value
        mock_source_client = mock_source_session.client.return_value
        mock_s3_rsrc = mock_source_session.resource.return_value
        mock_botocore_config = mock_botocore_config_constructor.return_value
        mock_destination_client = mock_boto3_client.Client.return_value
        mock_parser.parse_args.return_value = expected_args

        expected_add_argument_calls = [
            call(expected_positional_argument_source_bucket),
            call(expected_positional_argument_destination_bucket),
            call(expected_fast_access_flag_short, expected_fast_access_flag_full, action='store_true', help=expected_fast_access_flag_help),
        ]

        expected_bucket_exists_calls = [
            call(mock_source_client, expected_source_bucket),
            call(mock_destination_client, expected_destination_bucket),
        ]

        with patch('archive_copy.get_s3objects', return_value=expected_s3_objects) as mock_get_s3objects:
            # Act
            actual_exit_code = archive_copy.main()

            # Assert
            self.assertEqual(actual_exit_code, expected_exit_code)
            mock_argument_parser.assert_called_with(description=expected_description, epilog=expected_epilogue)
            mock_parser.add_argument.assert_has_calls(expected_add_argument_calls)
            mock_parser.print_usage.assert_called()
            mock_parser.parse_args.assert_called()
            mock_logging_basic_config.assert_called_with(level=archive_copy.LOG_LEVEL, format=expected_log_format)
            mock_botocore_config_constructor.assert_called_with(max_pool_connections=expected_max_pool_connections)
            mock_boto3_client.assert_called_with(expected_s3_service_name, config=mock_botocore_config)
            mock_boto3_session.assert_called_with(profile_name=expected_profile_name)
            mock_source_session.client.assert_called_with(expected_s3_service_name, config=mock_botocore_config)
            mock_bucket_exists.asset_has_calls(expected_bucket_exists_calls)
            mock_get_s3objects.assert_called_with(mock_source_client, expected_source_bucket)
            mock_count_remaining_and_request_restores.assert_called_with(
                mock_s3_rsrc,
                expected_source_bucket,
                expected_s3_objects
            )
            mock_copy_s3objects.assert_called_with(
                mock_source_client,
                expected_source_bucket,
                expected_destination_bucket,
                expected_s3_objects,
                expected_copy_to_glacier
            )

@patch('boto3.client')
class TestCopyS3Object(unittest.TestCase):
    def test_copy_s3object_fast_access(
            self,
            mock_boto3_client
    ):
        # Arrange
        mock_source_client = mock_boto3_client.return_value
        expected_source_bucket = 'my-old-archives'
        expected_dest_bucket = 'my-new-archives'
        expected_key = 'file1.csv'
        expected_s3object = {
            'Key': expected_key
        }
        expected_copy_to_glacier = False

        # Act
        archive_copy.copy_s3object(
            mock_source_client,
            expected_source_bucket,
            expected_dest_bucket,
            expected_copy_to_glacier,
            expected_s3object,
        )

        # Assert
        mock_source_client.copy.assert_called_with(
            CopySource={'Bucket': expected_source_bucket, 'Key': expected_key},
            Bucket=expected_dest_bucket,
            Key=expected_key,
            ExtraArgs=None
        )

    def test_copy_s3object_glacier(
            self,
            mock_boto3_client
    ):
        # Arrange
        mock_source_client = mock_boto3_client.return_value
        expected_source_bucket = 'my-old-archives'
        expected_dest_bucket = 'my-new-archives'
        expected_key = 'file1.csv'
        expected_s3object = {
            'Key': expected_key
        }
        expected_copy_to_glacier = True

        # Act
        archive_copy.copy_s3object(
            mock_source_client,
            expected_source_bucket,
            expected_dest_bucket,
            expected_copy_to_glacier,
            expected_s3object,
        )

        # Assert
        mock_source_client.copy.assert_called_with(
            CopySource={'Bucket': expected_source_bucket, 'Key': expected_key},
            Bucket=expected_dest_bucket,
            Key=expected_key,
            ExtraArgs={'StorageClass': 'GLACIER'}
        )

@patch('boto3.client')
@patch('archive_copy.ThreadPool')
@patch('archive_copy.partial')
@patch('archive_copy.copy_s3object')
class TestCopyS3Objects(unittest.TestCase):
    def test_copy_s3objects_fast_access(
            self,
            mock_copy_s3object,
            mock_partial,
            mock_thread_pool,
            mock_boto3_client
    ):
        # Arrange
        mock_source_client = mock_boto3_client.return_value
        expected_source_bucket = 'my-old-archives'
        expected_dest_bucket = 'my-new-archives'
        expected_s3objects = [{'Key': 'file1.csv'}, {'Key': 'file2.csv'}]
        mock_partial_copy_s3object = mock_partial.return_value
        mock_pool = mock_thread_pool.return_value
        expected_copy_to_glacier = False
        # Act
        archive_copy.copy_s3objects(
            mock_source_client,
            expected_source_bucket,
            expected_dest_bucket,
            expected_s3objects,
            expected_copy_to_glacier
        )
        # Assert
        mock_thread_pool.assert_called_with(processes=archive_copy.NUMBER_OF_COPY_THREADS)
        mock_partial.assert_called_with(
            mock_copy_s3object,
            mock_source_client,
            expected_source_bucket,
            expected_dest_bucket,
            expected_copy_to_glacier
        )
        mock_pool.map.assert_called_with(mock_partial_copy_s3object, expected_s3objects)

    def test_copy_s3objects_copy_to_glacier(
            self,
            mock_copy_s3object,
            mock_partial,
            mock_thread_pool,
            mock_boto3_client
    ):
        # Arrange
        mock_source_client = mock_boto3_client.return_value
        expected_source_bucket = 'my-old-archives'
        expected_dest_bucket = 'my-new-archives'
        expected_s3objects = [{'Key': 'file1.csv'}, {'Key': 'file2.csv'}]
        mock_partial_copy_s3object = mock_partial.return_value
        mock_pool = mock_thread_pool.return_value
        expected_copy_to_glacier = True
        # Act
        archive_copy.copy_s3objects(
            mock_source_client,
            expected_source_bucket,
            expected_dest_bucket,
            expected_s3objects,
            expected_copy_to_glacier
        )
        # Assert
        mock_thread_pool.assert_called_with(processes=archive_copy.NUMBER_OF_COPY_THREADS)
        mock_partial.assert_called_with(
            mock_copy_s3object,
            mock_source_client,
            expected_source_bucket,
            expected_dest_bucket,
            expected_copy_to_glacier
        )
        mock_pool.map.assert_called_with(mock_partial_copy_s3object, expected_s3objects)

if __name__ == '__main__':
    unittest.main()
