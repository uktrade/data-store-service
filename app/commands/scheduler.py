import click
from apscheduler.schedulers.background import BlockingScheduler
from data_engineering.common.application import get_or_create
from flask.cli import AppGroup

from app.uploader.utils import mark_old_processing_data_files_as_failed


@click.command('start')
def start_scheduler():
    """Start the job scheduler"""
    app = get_or_create()
    sched = BlockingScheduler(daemon=True)
    sched.add_job(
        mark_old_processing_data_files_as_failed, 'interval', minutes=60, kwargs=dict(app=app)
    )
    sched.start()


cmd_group = AppGroup('scheduler', help='Commands to manage the task scheduler')
cmd_group.add_command(start_scheduler)
