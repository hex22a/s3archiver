import os
import unittest
import sys
from os.path import join
from unittest.mock import patch, call, mock_open

import archive


@patch('logging.info')
@patch('logging.error')
@patch('logging.basicConfig')
@patch('archive.bucket_exists', return_value=True)
@patch('archive.put_file')
class TestArchive(unittest.TestCase):
    def test_main_3_args(
            self,
            mock_put_file,
            mock_bucket_exists,
            mock_logging_basic_config,
            mock_logging_error,
            mock_logging_info,
    ):
        # Arrange
        expected_filename_1 = 'file1.csv'
        expected_filename_2 = 'file2.csv'
        expected_file_path_1 = '/Documents/Folder/file1.csv'
        expected_file_path_2 = '/Documents/Folder/file2.csv'
        expected_paths = [(expected_filename_1, expected_file_path_1), (expected_filename_2, expected_file_path_2)]
        expected_script_name = './archive.py'
        expected_local_path = '/Documents/Folder'
        expected_s3_bucket = 'my-archive-bucket'
        expected_prefix = 'archive/2024'
        expected_args = [expected_script_name, expected_local_path, expected_s3_bucket, expected_prefix]
        expected_exit_code = 0
        expected_log_format = '%(levelname)s: %(asctime)s: %(message)s'

        expected_put_file_calls = [
            call(expected_file_path_1, expected_s3_bucket, expected_filename_1, expected_prefix),
            call(expected_file_path_2, expected_s3_bucket, expected_filename_2, expected_prefix)
        ]

        with patch.object(sys, 'argv', expected_args), patch('archive.get_paths',
                                                             return_value=expected_paths) as mock_get_paths:
            # Act
            actual_exit_code = archive.main()

            # Assert
            self.assertEqual(actual_exit_code, expected_exit_code)
            mock_logging_basic_config.assert_called_with(level=archive.LOG_LEVEL, format=expected_log_format)
            mock_get_paths.assert_called_with(expected_local_path)
            mock_put_file.assert_has_calls(expected_put_file_calls)

    def test_main_2_args(
            self,
            mock_put_file,
            mock_bucket_exists,
            mock_logging_basic_config,
            mock_logging_error,
            mock_logging_info,
    ):
        # Arrange
        expected_filename_1 = 'file1.csv'
        expected_filename_2 = 'file2.csv'
        expected_file_path_1 = '/Documents/Folder/file1.csv'
        expected_file_path_2 = '/Documents/Folder/file2.csv'
        expected_paths = [(expected_filename_1, expected_file_path_1), (expected_filename_2, expected_file_path_2)]
        expected_script_name = './archive.py'
        expected_local_path = '/Documents/Folder'
        expected_s3_bucket = 'my-archive-bucket'
        expected_prefix = ''
        expected_args = [expected_script_name, expected_local_path, expected_s3_bucket]
        expected_exit_code = 0
        expected_log_format = '%(levelname)s: %(asctime)s: %(message)s'

        expected_put_file_calls = [
            call(expected_file_path_1, expected_s3_bucket, expected_filename_1, expected_prefix),
            call(expected_file_path_2, expected_s3_bucket, expected_filename_2, expected_prefix)
        ]

        with patch.object(sys, 'argv', expected_args), patch('archive.get_paths',
                                                             return_value=expected_paths) as mock_get_paths:
            # Act
            actual_exit_code = archive.main()

            # Assert
            self.assertEqual(actual_exit_code, expected_exit_code)
            mock_logging_basic_config.assert_called_with(level=archive.LOG_LEVEL, format=expected_log_format)
            mock_get_paths.assert_called_with(expected_local_path)
            mock_put_file.assert_has_calls(expected_put_file_calls)


class TestGetPaths(unittest.TestCase):
    def test_get_paths(self):
        # Arrange
        expected_local_path = 'test_fixtures/upload_files'
        expected_filename_1 = 'file1.txt'
        expected_filename_2 = 'file2.txt'
        expected_file_paths = [
            (expected_filename_2, join(os.getcwd(), expected_local_path, expected_filename_2)),
            (expected_filename_1, join(os.getcwd(), expected_local_path, expected_filename_1)),
        ]

        # Act
        actual_file_paths = archive.get_paths(expected_local_path)

        # Assert
        self.assertEqual(expected_file_paths, actual_file_paths)


@patch('archive.s3')
@patch("builtins.open", new_callable=mock_open, read_data="data")
class TestPutFile(unittest.TestCase):
    def test_put_file(
            self,
            mock_file,
            mock_s3,
    ):
        # Arrange
        expected_prefix = 'archive/2024'
        expected_filename = 'file.txt'
        expected_s3_bucket = 'my-archive-bucket'
        expected_key = 'file.txt'
        expected_acl = 'private'
        expected_storage_class = 'GLACIER'
        expected_flags = 'rb'

        # Act
        archive.put_file(expected_filename, expected_s3_bucket, expected_key, expected_prefix)

        # Assert
        mock_file.assert_called_with(expected_filename, expected_flags)
        mock_s3.put_object.assert_called_with(Body=open(expected_filename, expected_flags), ACL=expected_acl, Key=f'{expected_prefix}/{expected_key}', Bucket=expected_s3_bucket, StorageClass=expected_storage_class)

if __name__ == '__main__':
    unittest.main()
