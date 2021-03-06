# pylint: skip-file
"""
Contains configurations and settings used by the rest of the project.

Any settings in here can be overriden by config_private.py.
"""
import functools
import getpass
import os
import pathlib
from typing import Any, Callable

import stackprinter

stackprinter.set_excepthook(style="darkbg")  # easier to read stack traces

_PROJECT_DIR = pathlib.Path(__file__).resolve().parent.parent

DATA_DIR = _PROJECT_DIR / "data"
RESOURCE_DIR = _PROJECT_DIR / "resources"

# CAS Authentication
#
# When off campus and not connected to the Yale VPN, you must use cookie-based authentication.
# The credentials are only used if CAS_USE_COOKIE is set to False, and the cookie data is
# only used when CAS_USE_COOKIE is set to True.
#
# The latter three settings can also be set to a function. The function should return the
# specified setting when called.
CAS_USE_COOKIE = True
CAS_CREDENTIAL_NETID: Callable[[Any], str] = functools.lru_cache(
    lambda: input("Yale NetId: ")
)
CAS_CREDENTIAL_PASSWORD: Callable[[Any], str] = functools.lru_cache(getpass.getpass)
CAS_COOKIE_TGC: Callable[[Any], str] = functools.lru_cache(
    lambda: input("TGC Cookie: ")
)


# Database
DATABASE_CONNECT_STRING = os.environ.get(
    "POSTGRES_URI", "postgresql+psycopg2://postgres:thisisapassword@localhost/postgres"
)

# Enable overrides from config_private.py
try:
    from ferry.config_private import *
except ModuleNotFoundError:
    pass
