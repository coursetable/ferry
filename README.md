# Ferry v2 - **UNDER DEVELOPMENT**

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

TODO:
 - **Ensure that parallel ratings is accurate**
 - Modernize dependencies
 - Error handling on legacy artifacts in dev (old discussion sections, demand stats, and migrated courses)
 - SIGINT handler
 - Containerize ferry
    - Full Doppler integration
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
