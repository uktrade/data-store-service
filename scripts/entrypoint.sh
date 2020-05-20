#!/bin/bash

source ./scripts/functions.sh

# run the scheduled task to populate the database in the celery app
run "./scripts/compile_assets.sh"
run "make run_dev_server"
