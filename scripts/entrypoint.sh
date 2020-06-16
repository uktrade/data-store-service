#!/bin/bash

source ./scripts/functions.sh

# run the scheduled task to populate the database in the celery app
run "./manage.py dev db --run_migrations"
run "./scripts/compile_assets.sh"
run "make run_dev_server"
