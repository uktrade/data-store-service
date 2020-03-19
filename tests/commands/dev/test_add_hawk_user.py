from unittest import mock

import pytest

from app.commands.dev.add_hawk_user import add_hawk_user


class TestAddHawkUserCommand:
    def test_add_hawk_cmd_help(self, app_with_db):
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(add_hawk_user)
        assert 'Usage: add_hawk_user [OPTIONS]' in result.output
        assert result.exit_code == 0
        assert result.exception is None

    @pytest.mark.parametrize(
        'client_id,expected_add_user_called', (('client_id', True), (None, False),),
    )
    @mock.patch('common.db.models.HawkUsers.add_user')
    def test_run_hawk_user(self, mock_add_user, client_id, expected_add_user_called, app_with_db):
        mock_add_user.return_value = None
        runner = app_with_db.test_cli_runner()

        args = [
            '--client_key',
            'client_key',
            '--client_scope',
            'client_scope',
            '--description',
            'description',
        ]
        if client_id:
            args.extend(['--client_id', client_id])

        result = runner.invoke(add_hawk_user, args,)
        assert mock_add_user.called is expected_add_user_called
        if not expected_add_user_called:
            assert result.output.startswith('All parameters are required')
