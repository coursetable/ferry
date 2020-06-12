# Ferry
A crawler for Yale courses and evaluation data. Integrates with Coursetable.

## Setup
Run `poetry install` to setup the virtualenv, install dependencies, and bootstrap the project.

## TODO
eventual
- automatically apply black and isort
- save the raw HTML from OCE in the evaluations crawler
- compatability layer shims for existing coursetable
- use poetry scripts for easier execution
- transition everything from /private to config_private.py
- add database config into config.py

current:
- [harshal] script to update all computed fields across the database
- [harshal] script to check all database invariants
- [kevin] integration with google drive for data
- [kevin] rerun the full pipeline with a clean slate
