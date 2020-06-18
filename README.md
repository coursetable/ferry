# Ferry
A crawler for Yale courses and evaluation data. Integrates with Coursetable.

## Setup
Run `poetry install` to setup the virtualenv, install dependencies, and bootstrap the project.

The following dependencies are required besides the ones handled by Poetry (if these are not installed, Poetry may report some errors)

- We use the `graphviz` module to generate schema graphs, which requires additional libraries to be installed. On macOS, run `brew install graphviz`, and on Linux, run `sudo apt-get install python3.8-dev graphviz libgraphviz-dev pkg-config`.
- The server is implemented in `postgresql`. To install, run `brew install postgresql` on macOS, `sudo apt-get install libpq-dev` on Ubuntu.
- On post-Sierra versions of macOS, running `poetry install` may report an error during `psycopg2` installation stating that `ld: library not found for -lssl`. To fix this, make sure OpenSSL installed (for instance, after running `brew install openssl`), and add its libraries to the path with `export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/`.

## Data Files
Some of the intermediate JSON files generated by the crawler and migrator in `/api_output` take several hours to fetch from the Yale API and/or CourseTable.
We have uploaded these compressed folders to a [Google Drive folder](https://drive.google.com/drive/u/1/folders/14wl5ibpeLTQaVHK-DNTfLUaWb1N7lY7M).

[optional] We have a couple scripts to automate this process - they currently require `rclone` to be [installed](https://rclone.org/install/). Once this is set up, run `rclone config` to set up the remote, which must be called `coursetable`. The client ID and client secret can be accessed via this [OAuth client](https://console.developers.google.com/apis/credentials/oauthclient/834119546246-kga3min5p74ks3rdmceu68librsfj5oc.apps.googleusercontent.com?project=ferry-280404&supportedpurview=project) (ask for access). Use Coursetable's Google Account to authenticate with rclone.
**To pull the files**, run the `drive_fetch.sh` script.
**To update the files in drive**, run the `drive_push.sh` script.

## TODO
eventual
- automatically apply black and isort
- save the raw HTML from OCE in the evaluations crawler
- compatability layer shims for existing coursetable
- use poetry scripts for easier execution
- transition everything from /private to config_private.py
- add database config into config.py
- in `fetch_ratings.py`, change the courses list from `listings_with_extra_info.csv` to the outputs from `fetch_classes.py`

current:
- [kevin] rerun the full pipeline with a clean slate
