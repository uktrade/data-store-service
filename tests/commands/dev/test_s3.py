from unittest import mock

from app.commands.dev.s3 import s3
from app.commands.dev.s3 import delete_object, list_objects, move_object, upload_object


class TestDeleteObject:
    def test(self):
        bucket = 'bucket'
        client = mock.Mock()
        key = 'key'
        delete_object(bucket, client, key)
        client.delete_object.assert_called_once_with(Bucket=bucket, Key=key)


class TestListObject:
    def test(self):
        bucket = 'bucket'
        client = mock.Mock()
        list_objects(bucket, client)
        client.list_objects.assert_called_once_with(Bucket=bucket)


class TestMoveObject:
    def test(self):
        bucket = 'bucket'
        client = mock.Mock()
        key_from = 'key1'
        key_to = 'key2'
        move_object(bucket, client, key_from, key_to)
        client.copy.assert_called_once_with(
            CopySource={'Bucket': 'bucket', 'Key': 'key1'}, Bucket='bucket', Key='key2'
        )
        client.delete_object.assert_called_once_with(Bucket='bucket', Key='key1')


class TestUploadObject:
    def test(self):
        bucket = 'bucket'
        client = mock.Mock()
        path_from = 'from'
        path_to = 'to'
        upload_object(bucket, client, path_from, path_to)
        client.upload_file.assert_called_once_with('from', 'bucket', 'to')


@mock.patch('app.commands.dev.s3.os')
@mock.patch('app.commands.dev.s3.boto3')
class TestS3Command:
    def setup(self):
        self.bucket_name = 'bucket_name'
        self.client = mock.Mock()
        self.environ = {'AWS_BUCKET_NAME': self.bucket_name}

    def test_help(self, boto3, os, app_with_db):
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(s3)
        assert 'CRUD commands for s3' in result.output
        assert result.exit_code == 0
        assert result.exception is None

    @mock.patch('app.commands.dev.s3.delete_object')
    def test_delete(self, delete_object, boto3, os, app_with_db):
        os.environ = self.environ
        boto3.client.return_value = self.client
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(s3, ['delete', 'key'])
        assert result.exit_code == 0
        assert result.exception is None
        delete_object.assert_called_once_with(self.bucket_name, self.client, 'key')

    @mock.patch('app.commands.dev.s3.list_objects')
    def test_ls(self, list_objects, boto3, os, app_with_db):
        os.environ = self.environ
        boto3.client.return_value = self.client
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(s3, ['ls'])
        assert result.exit_code == 0
        assert result.exception is None
        list_objects.assert_called_once_with(self.bucket_name, self.client)

    @mock.patch('app.commands.dev.s3.move_object')
    def test_move(self, move_object, boto3, os, app_with_db):
        os.environ = self.environ
        boto3.client.return_value = self.client
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(s3, ['move', 'key1', 'key2'])
        assert result.exit_code == 0
        assert result.exception is None
        move_object.assert_called_once_with(self.bucket_name, self.client, 'key1', 'key2')

    @mock.patch('app.commands.dev.s3.upload_object')
    def test_upload(self, upload_object, boto3, os, app_with_db):
        os.environ = self.environ
        boto3.client.return_value = self.client
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(s3, ['upload', 'source', 'destination'])
        assert result.exit_code == 0
        assert result.exception is None
        upload_object.assert_called_once_with(
            self.bucket_name, self.client, 'source', 'destination'
        )
