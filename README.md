# Ferry
[![Maintainability](https://api.codeclimate.com/v1/badges/5f6f9f67a8ee990c41c3/maintainability)](https://codeclimate.com/github/coursetable/ferry/maintainability) [![HitCount](http://hits.dwyl.com/coursetable/ferry.svg)](http://hits.dwyl.com/coursetable/ferry)

A crawler for Yale courses and evaluation data. Integrates with [CourseTable](https://github.com/coursetable/coursetable).

## Repository layout

The crawler scripts are structured as follows:

- `/data`: Data files output by scraping and preprocessing. This folder is a private submodule because it contains course evaluations.
- `/docs`: Documentation on how the crawler works.
- `/ferry`: The primary crawler module. Besides the main scripts for database import after crawled files have been fetched and preprocessed, this module contains the following folders:
  - `/ferry/crawler`: Scripts for crawling Yale's various course information sites.
  - `/ferry/database`: The SQLAlchemy database model and configuration.
  - `/ferry/embed`: Scripts for generating and computing Word2Vec and TF-IDF embeddings from course titles and descriptions.
  - `/ferry/includes`: Helper functions used across Ferry.
  - `/ferry/migration`: Scripts we used to migrate the old (pre-summer 2020) database to the current one. No longer maintained, but retained just in case.
  - `/nlp`: Past scripts used for NLP analysis of course titles, descriptions, and evaluations.

## Architecture

The below diagram summarizes the workflow used to produce the final database. We run a subset of this pipeline every day as a cron job, which is performed by [refresh_courses.sh](refresh_courses.sh). See [the workflow overview](docs/0_workflow.md) for a more detailed description of each component.

![architecture](./docs/architecture.png)

(see also: [PDF version](./docs/architecture.pdf))

## How to develop

Check out [the getting started guide](docs/getting_started.md).

## Contributing

**Contributing code**:

1. Create a branch with your feature. This can usually be done with `git checkout -b <username>/<feature_name>`.
2. _make changes_.
3. Create some commits and push your changes to the origin (i.e. GitHub). If your branch does not yet exist on GitHub, use `git push --set-upstream origin <branch_name>`.
4. Once you believe your changes are ready to be integrated into main/master, create a pull request and add a few reviewers. In the pull request, be sure to reference any relevant issue numbers.
5. Once the pull request has been approved, merge it into the master branch.

**Considerations:**

- If modifying the database schema, make sure to run `/ferry/generate_db_diagram.py` to update the [database diagram](docs/db_diagram.png).
- If making major changes to the overall workflow (i.e. adding another script, data source, or removing a module) please update the [architecture diagram](docs/architecture.pdf) accordingly.

**Style**:

- For general code formatting, we use [black](https://github.com/psf/black).
- For import ordering, we use [isort](https://github.com/PyCQA/isort).
- For general code quality, we use [pylint](https://github.com/PyCQA/pylint).
- For static type checking, we use [mypy](http://mypy-lang.org/).
- For general clean code, we use [DeepSource](https://deepsource.io/).

Checks can be run manually from the repository root via

```bash
poetry run black ./ferry
poetry run isort ./ferry
poetry run pylint ./ferry
poetry run mypy ./ferry
```

All of these checks are run after each push and pull request with GitHub actions. Please make sure all checks pass before merging into master.

Plugins are also available for text editors such as VS Code and Sublime, which can make the development experience much smoother.

## Data files

The data files – outputs from the extraction stage – are stored in the `/data` directory.
The course evaluations data are private and should only be accessible to Yale students and faculty. As such, we store these files in a private Git submodule.

This submodule includes course and evaluation data dating back to 2009 (many of which are no longer available through Yale), more recent course demand statistics, and caches and raw HTML files for debugging purposes.

_If you want to use these data but don't want to crawl it yourself, please reach out and we can grant access to our archives._
