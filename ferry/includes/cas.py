"""
Functions for authenticating with Yale Central Authentication System (CAS).

Used by config.py.
"""

import requests

from ferry import config
from ferry.includes.utils import resolve_potentially_callable


def _create_session_from_credentials(net_id: str, password: str) -> requests.Session:
    session = requests.Session()

    # Login to CAS.
    _ = session.post(
        f"https://secure.its.yale.edu/cas/login?username={net_id}&password={password}"
    )

    # Verify that it worked.
    if "TGC" not in session.cookies.get_dict():
        raise NotImplementedError("cannot handle 2-factor authentication")

    return session


def _create_session_from_cookie(tgc: str) -> requests.Session:
    session = requests.Session()

    # Set user-agent for requests to work
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0"  # pylint: disable=line-too-long
        }
    )

    # Manually set cookie.
    cookie = requests.cookies.create_cookie(
        domain="secure.its.yale.edu",
        name="TGC",
        value=tgc,
        path="/cas/",
        secure=True,
    )
    session.cookies.set_cookie(cookie)

    return session


def create_session() -> requests.Session:
    """
    Create a session using parameters from /ferry/config.py.
    """

    if config.CAS_USE_COOKIE:
        tgc = resolve_potentially_callable(config.CAS_COOKIE_TGC)
        return _create_session_from_cookie(tgc)

    net_id = resolve_potentially_callable(config.CAS_CREDENTIAL_NETID)
    password = resolve_potentially_callable(config.CAS_CREDENTIAL_PASSWORD)
    return _create_session_from_credentials(net_id, password)


if __name__ == "__main__":
    # Create a session twice to test resolution.
    session_test = create_session()
    print(session_test)
    session_test = create_session()
    print(session_test)
    print("Cookies: ", session_test.cookies.get_dict())

    res = session_test.get("https://secure.its.yale.edu/cas/login")
    if res.text.find("Login Successful") < 0:
        print("failed to login")
        breakpoint()
    else:
        print("success")
