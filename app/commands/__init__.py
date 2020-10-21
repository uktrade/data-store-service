from app.commands.dev import cmd_group as dev_cmd_group
from app.commands.scheduler import cmd_group as scheduler_cmd_group


def get_command_groups():
    return [dev_cmd_group, scheduler_cmd_group]
