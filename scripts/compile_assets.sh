#!/bin/bash

source ./scripts/functions.sh

echo `more package.json`
run "npm install"
run "npm run build"
