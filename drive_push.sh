#!/bin/bash

echo 'Generating compressed data bundle'
tar -czf drive_cache.tar.gz api_output

echo 'Uploading to storage'
rclone copy drive_cache.tar.gz coursetable:ferry-data
