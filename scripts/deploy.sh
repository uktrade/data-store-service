#!/bin/bash

source ./scripts/functions.sh

PARAMETERS="-m 2G -k 2G"
SYSTEM="-$1"

if [ "$1" = "live" ]; then
  SYSTEM=""
fi

run "cf push -f manual-manifest.yml data-store-service$SYSTEM $PARAMETERS --no-start"
run "cf start data-store-service$SYSTEM"
