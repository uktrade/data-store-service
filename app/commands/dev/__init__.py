from flask.cli import AppGroup
from app.commands.dev.add_hawk_user import add_hawk_user as add_hawk_user_command
from app.commands.dev.datafiles_to_events_by_source import datafiles_to_events_by_source as datafiles_command
from app.commands.dev.db import db as db_command
from app.commands.dev.s3 import s3 as s3_command


cmd_group = AppGroup('dev', help='Commands to build database')

cmd_group.add_command(add_hawk_user_command)
cmd_group.add_command(datafiles_command)
cmd_group.add_command(db_command)
cmd_group.add_command(s3_command)
