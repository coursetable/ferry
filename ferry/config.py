import functools
import getpass
import os

"""
Contains configurations and settings used by the rest of the project.
Any settings in here can be overriden by config_private.py.
"""

_PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

DATA_DIR = os.path.join(_PROJECT_DIR, "api_output")
RESOURCE_DIR = os.path.join(_PROJECT_DIR, "resources")

# CAS Authentication
#
# When off campus and not connected to the Yale VPN, you must use cookie-based authentication.
# The credentials are only used if CAS_USE_COOKIE is set to False, and the cookie data is
# only used when CAS_USE_COOKIE is set to True.
#
# The latter three settings can also be set to a function. The function should return the
# specified setting when called.
CAS_USE_COOKIE = True
CAS_CREDENTIAL_NETID = functools.lru_cache(lambda: input("Yale NetId: "))
CAS_CREDENTIAL_PASSWORD = functools.lru_cache(lambda: getpass.getpass())
CAS_COOKIE_CASTGC = functools.lru_cache(lambda: input("CASTGC Cookie: "))


# Enable overrides from config_private.py
try:
    from ferry.config_private import *
except ModuleNotFoundError:
    pass