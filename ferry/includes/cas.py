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

    ###

    tgc = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.ZXlKNmFYQWlPaUpFUlVZaUxDSmhiR2NpT2lKa2FYSWlMQ0psYm1NaU9pSkJNVEk0UTBKRExVaFRNalUySWl3aVkzUjVJam9pU2xkVUlpd2lkSGx3SWpvaVNsZFVJbjAuLmo3Y3ZfeHU2elA0ZWRmY0ROUkJGZHcubUx0LUJzeHVPWktWVWJxLTdOUk9vMDVjN0VHeENQak5iZGt6bWFqU2hxR3R6UTJuM2ZhU0xTams2Z0dLNmY3VW1xaWJwbU9uWDd3YnUyTzZXYWxnc0RCc3psMmlDaFpZcU92Y3EyMWRsZ3hFWGVDVXJHcUNtSUFKM19zOGhpa19hWm5sbG5rMUtsb0gtaFNOdXdsTjdCNkZkVm4wdkZucnhEelFxNm1TdW5wWkZIYlZrRGRLNlVYVHNoWXh3UFp4eVhzWHJfbEdXN05ZVmV6SE5xMkJmUDRKd0UzM1NEWFNmbkNHcmE1NTR2cmZ6QV9aUEx6U19KZi1EYzRPSFhrWDRBd1dxRU1xRFBPVG15WUhCdUNNbHcubkF4QlpRUjYtbkpqY3ZqblVGdzZMUQ.7Bp0hj_CphM-ELERB7eX2kvpAWmSbSTlVgddsz4F7UmdvGp5syV2-PSMXeTo4MT4kX_teiO7htjHR12BRhv4XA"
    jsessionid = "D5344130655D9BCAB2728F4EA7EB1B33"

    # Set user-agent for requests to work
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36"  # pylint: disable=line-too-long
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

    cookie_2 = requests.cookies.create_cookie(
        domain="secure.its.yale.edu",
        name="JSESSIONID",
        value=jsessionid,
        path="/cas/",
        secure=True,
    )
    session.cookies.set_cookie(cookie_2)

    ####

    # Verify that it worked.
    if "TGC" not in session.cookies.get_dict():
        raise NotImplementedError("cannot handle 2-factor authentication")

    return session


def _create_session_from_cookie(tgc: str) -> requests.Session:
    session = requests.Session()

    # Set user-agent for requests to work
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36"  # pylint: disable=line-too-long
        }
    )

    tgc = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.ZXlKNmFYQWlPaUpFUlVZaUxDSmhiR2NpT2lKa2FYSWlMQ0psYm1NaU9pSkJNVEk0UTBKRExVaFRNalUySWl3aVkzUjVJam9pU2xkVUlpd2lkSGx3SWpvaVNsZFVJbjAuLmo3Y3ZfeHU2elA0ZWRmY0ROUkJGZHcubUx0LUJzeHVPWktWVWJxLTdOUk9vMDVjN0VHeENQak5iZGt6bWFqU2hxR3R6UTJuM2ZhU0xTams2Z0dLNmY3VW1xaWJwbU9uWDd3YnUyTzZXYWxnc0RCc3psMmlDaFpZcU92Y3EyMWRsZ3hFWGVDVXJHcUNtSUFKM19zOGhpa19hWm5sbG5rMUtsb0gtaFNOdXdsTjdCNkZkVm4wdkZucnhEelFxNm1TdW5wWkZIYlZrRGRLNlVYVHNoWXh3UFp4eVhzWHJfbEdXN05ZVmV6SE5xMkJmUDRKd0UzM1NEWFNmbkNHcmE1NTR2cmZ6QV9aUEx6U19KZi1EYzRPSFhrWDRBd1dxRU1xRFBPVG15WUhCdUNNbHcubkF4QlpRUjYtbkpqY3ZqblVGdzZMUQ.7Bp0hj_CphM-ELERB7eX2kvpAWmSbSTlVgddsz4F7UmdvGp5syV2-PSMXeTo4MT4kX_teiO7htjHR12BRhv4XA"
    jsessionid = "D5344130655D9BCAB2728F4EA7EB1B33"

    # Manually set cookie.
    cookie = requests.cookies.create_cookie(
        domain="secure.its.yale.edu",
        name="TGC",
        value=tgc,
        path="/cas/",
        secure=True,
    )
    session.cookies.set_cookie(cookie)

    cookie_2 = requests.cookies.create_cookie(
        domain="secure.its.yale.edu",
        name="JSESSIONID",
        value=jsessionid,
        path="/cas/",
        secure=True,
    )
    session.cookies.set_cookie(cookie_2)

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
        breakpoint()  # pylint: disable=forgotten-debug-statement
    else:
        print("success")
