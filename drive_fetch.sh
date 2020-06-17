#!/bin/bash

echo 'Pulling data from storage'
rclone copy coursetable:ferry-data/drive_cache.tar.gz . --progress

echo 'Extracting from archive'
tar -xvf drive_cache.tar.gz
