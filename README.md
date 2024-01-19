# Ferry v2 - **UNDER DEVELOPMENT**

TODO:
 - Test database related transforms + scripts
 - Remove unnecessary NLP stuff
 - Full Doppler integration
 - Containerize ferry
 - Write documentation, standardize variable names, cleanup, etc.
 - Cleanup those branches
 - Lint and format CI

DONE:
 - Modernized driver
 - Modernized module structure
    - `main.py` controls everything
 - Pretty printing
    - Managed `tqdm` progress bars and rich output
 - Modernized args
    - `argparse` with config file support
 - Season, class, and ratings fetch are now parallel (`async`)
 - Season, class, and ratings parse are now parallel (`multiprocessing`)
 - Consistent and modern utility functions
    - `pathlib.Path` everywhere, etc.
 - No more `poetry`
    - Just use `pip` with frozen requirements