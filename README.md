# Ferry v2 - **UNDER DEVELOPMENT**

[![Ferry Run](https://github.com/coursetable/ferry/actions/workflows/ferry.yml/badge.svg)](https://github.com/coursetable/ferry/actions/workflows/ferry.yml)

## Installation

1. Ferry uses [VSCode's Dev Container](https://code.visualstudio.com/docs/devcontainers/containers) for the dev environment. All environment setup is handled through the standard Dev Container pipeline. For a quick start, use the `Dev Containers: Reopen in Container` command in VSCode.

1. _MacOS_: If you want to develop outside of the Dev Container, please install `pygraphviz` first before running `pip install -e .`

   ```sh
   pip install -U --no-cache-dir  \
               --config-settings="--global-option=build_ext" \
               --config-settings="--global-option=-I$(brew --prefix graphviz)/include/" \
               --config-settings="--global-option=-L$(brew --prefix graphviz)/lib/" \
               pygraphviz
   ```

## Running Ferry

Make sure you have all dependencies installed or are using the Dev Container.

```sh
python main.py -f config/dev_fetch.yml
```

### Options

Options can be passed either through the command line, through a YAML config file, or through environment variables, in decreasing order of precedence.

Note: because we use `argparse`, you can provide just the prefix of each argument, such as `--config` instead of ``--config-file`.

| CLI flag                    | Config option             | Env key        | Default                              | Description                                                                                           |
| --------------------------- | ------------------------- | -------------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `--cas-cookie`              | `cas_cookie`              | `CAS_COOKIE`   | `None`; prompt if `fetch_evals`      | Only used for fetching evals; see below                                                               |
| `-f`, `--config-file`       | N/A                       | N/A            | `None`                               | Path to YAML config file, relative to PWD; if unspecified, all options are read from command          |
| `--data-dir`                | `data_dir`                | N/A            | `data`                               | Directory to load/store parsed data. This is usually where the `ferry-data` is cloned.                |
| `--database-connect-string` | `database_connect_string` | `POSTGRES_URI` | `None`; prompt if `sync_db`          | Postgres connection string; for dev, see `dev_sync_db.yml`                                            |
| `-d`, `--debug`             | `debug`                   | N/A            | `False`                              | Enable debug logging                                                                                  |
| `--fetch-classes`           | `fetch_classes`           | N/A            | `False`                              | Run the class crawler                                                                                 |
| `--fetch-evals`             | `fetch_evals`             | N/A            | `False`                              | Run the eval crawler                                                                                  |
| `--generate-diagram`        | `generate_diagram`        | N/A            | `False`                              | Generate a DB visualization diagram to `docs/db_diagram.pdf`                                          |
| `--parse-evals`             | `parse_evals`             | N/A            | `False`                              | Run the eval crawler, but just parse existing fetched evals                                           |
| `-r`, `--release`           | `release`                 | N/A            | `False`                              | Run in release mode; see below                                                                        |
| `--save-config`             | `save_config`             | N/A            | `False`                              | Save the parsed config options to `config_file`; does nothing if `config_file` is unspecified.        |
| `-s`, `--seasons`           | `seasons`                 | N/A            | `None`                               | A list of seasons to fetch; see below                                                                 |
| `--sentry-url`              | `sentry_url`              | `SENTRY_URL`   | `None`; prompt if `release`          | Sentry URL for error reporting; required in release mode, ignored in dev mode                         |
| `--sync-db`                 | `sync_db`                 | N/A            | `False`                              | Sync the parsed data to the database                                                                  |
| `--use-cache`               | `use_cache`               | N/A            | `False`; always `False` if `release` | Use cached data instead of fetching fresh data. Even if not using cache, cache will still be updated. |

### Release mode

In release mode:

- `use_cache` is always `False`; we always fetch fresh data.
- `sentry_url` is required; we will send error reports to Sentry.

In non-release mode:

- We never initialize Sentry and `sentry_url` is ignored.

### CAS cookie

To get a valid CAS cookie to connect to OCE, first log into https://oce.app.yale.edu/ocedashboard/studentViewer. Refresh with the network inspector tab open, and find the first HTTP request. Copy the `Cookie` header from the request and paste into the prompt or the `cas_cookie` option. It should look like:

```plain
JSESSIONID=...; ...
```

### Specifying seasons

- If the `seasons` option is unspecified, we default to fetching ALL viable seasons.
- If the `seasons` option contains a single value called `LATEST_n`, we fetch the latest `n` seasons.
- Otherwise, the `seasons` option should be a list of seasons to fetch, using the standard season code format: e.g. `--seasons 202301 202303`.

**In almost all cases, it is sufficient to only fetch the last 3 seasons in dev.** In fact, `ferry` will not work when fetching and syncing to DB all seasons in dev due to `professor_id` mapping requiring legacy seasons. Please clone [`ferry-data`](https://github.com/coursetable/ferry-data) if working with all seasons is necessary.

## Linting & formatting

```sh
black .
pyright
```

## TODO

- **Ensure that parallel ratings is accurate**
- Rearchitect GitHub action to create Postgres dump, upload with VC (enable lifecycle management) to Azure Blob, and restart prod `postgres` container to trigger new pull.
- Modernize dependencies
- SIGINT handler
- Write documentation, standardize variable names, cleanup, etc.
- Cleanup those branches
- Lint and format CI
- Parallelize db sync scripts (stage, transform, deploy)
