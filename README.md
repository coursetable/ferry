# Ferry v2 - **UNDER DEVELOPMENT**

[![Ferry Run](https://github.com/coursetable/ferry/actions/workflows/ferry.yml/badge.svg)](https://github.com/coursetable/ferry/actions/workflows/ferry.yml)

Contributing
1. Ferry uses [VSCode's Dev Container](https://code.visualstudio.com/docs/devcontainers/containers) for the dev environment. All environment setup is handled through the standard Dev Container pipeline.

1. *MacOS*: If you want to develop outside of the Dev Container, please install `pygraphviz` first before running `pip install -e .`
   ```sh
   pip install -U --no-cache-dir  \
               --config-settings="--global-option=build_ext" \
               --config-settings="--global-option=-I$(brew --prefix graphviz)/include/" \
               --config-settings="--global-option=-L$(brew --prefix graphviz)/lib/" \
               pygraphviz
   ```

1. **In almost all cases, it is sufficient to only fetch the last 3 seasons in dev.** In fact, `ferry` will not work when fetching and syncing to DB all seasons in dev due to `professor_id` mapping requiring legacy seasons. Please clone [`ferry-data`](https://github.com/coursetable/ferry-data) if working with all seasons is necessary.

TODO:
 - **Ensure that parallel ratings is accurate**
 - Rearchitect GitHub action to create Postgres dump, upload with VC (enable lifecycle management) to Azure Blob, and restart prod `postgres` container to trigger new pull.
 - Modernize dependencies
 - SIGINT handler
 - Write documentation, standardize variable names, cleanup, etc.
 - Cleanup those branches
 - Lint and format CI
 - Parallelize db sync scripts (stage, transform, deploy)

DONE:
 - Modernized driver
 - Modernized module structure
    - `main.py` controls everything
 - Pretty printing
    - Managed `tqdm` progress bars and rich output
 - Modernized args
    - `argparse` with config file support
 - Season, class, and ratings fetch are now parallel (`async`)
    - Ratings fetch uses lambda invocation to emulate native concurrency because CAS cookie is unstable otherwise.
    - See [`lambda/`](https://github.com/coursetable/ferry/blob/v2/lambda/README.md) for details.
 - Season, class, and ratings parse are now parallel (`multiprocessing`)
 - Consistent and modern utility functions
    - `pathlib.Path` everywhere, etc.
 - No more `poetry`
    - Just use `pip` with frozen requirements
 - Automated cron job Ferry run with GitHub Actions
    - Containerized for dev