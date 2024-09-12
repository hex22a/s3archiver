import unittest
import sys
from unittest.mock import patch, call

import archive_copy


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
            mock_boto3_session
    ):
        # Arrange
        expected_s3_objects = ['file1.csv', 'file2.csv']
        expected_script_name = './archive_copy.py'
        expected_source_bucket = 'my-old-archives'
        expected_destination_bucket = 'my-new-archives'
        expected_args = [expected_script_name, expected_source_bucket, expected_destination_bucket]
        expected_exit_code = 0
        expected_log_format = archive_copy.LOG_FORMAT
        expected_s3_service_name = archive_copy.S3_SERVICE_NAME
        expected_profile_name = archive_copy.SOURCE_ARCHIVE_PROFILE_NAME
        expected_max_pool_connections = archive_copy.MAX_POOL_CONNECTIONS

        mock_source_session = mock_boto3_session.return_value
        mock_source_client = mock_source_session.client.return_value
        mock_s3_rsrc = mock_source_session.resource.return_value
        mock_botocore_config = mock_botocore_config_constructor.return_value
        mock_destination_client = mock_boto3_client.Client.return_value

        expected_bucket_exists_calls = [
            call(mock_source_client, expected_source_bucket),
            call(mock_destination_client, expected_destination_bucket),
        ]

        with patch.object(sys, 'argv', expected_args), patch('archive_copy.get_s3objects',
                                                             return_value=expected_s3_objects) as mock_get_s3objects:
            # Act
            actual_exit_code = archive_copy.main()

            # Assert
            self.assertEqual(actual_exit_code, expected_exit_code)
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
                expected_s3_objects
            )


if __name__ == '__main__':
    unittest.main()
