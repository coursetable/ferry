# Ferry v2 - **UNDER DEVELOPMENT**

INSTALLATION:
1. Install [`graphviz`](https://www.graphviz.org/) for your platform
   - *MacOS*: Install with `brew` and install `pygraphviz`
      ```sh
      brew install graphviz
      python -m pip install \
      --global-option=build_ext \
      --global-option="-I$(brew --prefix graphviz)/include/" \
      --global-option="-L$(brew --prefix graphviz)/lib/" \
      pygraphviz
      ```
1. Install `ferry` in editable mode
   ```sh
   pip install -e .
   ```

TODO:
 - Error handling on legacy artifacts in dev (old discussion sections, demand stats, and migrated courses)
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
