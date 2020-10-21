import click
from apscheduler.schedulers.background import BlockingScheduler
from flask import current_app
from flask.cli import AppGroup, with_appcontext

from app.uploader.utils import mark_old_processing_data_files_as_failed


@click.command('start')
@with_appcontext
def start_scheduler():
    """Start the job scheduler"""
    sched = BlockingScheduler(daemon=True)
    sched.add_job(
        mark_old_processing_data_files_as_failed,
        'interval',
        kwargs=dict(app=current_app),
        minutes=60,
    )
    sched.start()


cmd_group = AppGroup('scheduler', help='Commands to manage the task scheduler')
cmd_group.add_command(start_scheduler)
