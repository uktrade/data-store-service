#!/bin/bash

# For some reason on CF the symbolic link to chromium-browser in the path is setup incorrectly

rm /home/vcap/deps/0/bin/chromium-browser
ln -s /home/vcap/deps/0/lib/chromium-browser/chromium-browser /home/vcap/deps/0/bin/chromium-browser
