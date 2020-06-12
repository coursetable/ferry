import requests

from ferry import config
from ferry.includes.utils import resolve_potentially_callable


def _create_session_from_credentials(netId, password):
    session = requests.Session()

    # Login to CAS.
    _ = session.post(
        f"https://secure.its.yale.edu/cas/login?username={netId}&password={password}"
    )

    # Verify that it worked.
    if "CASTGC" not in session.cookies.get_dict():
        raise NotImplementedError("cannot handle 2-factor authentication")

    return session


def _create_session_from_cookie(castgc):
    session = requests.Session()

    # Manually set cookie.
    cookie = requests.cookies.create_cookie(
        domain="secure.its.yale.edu",
        name="CASTGC",
        value=castgc,
        path="/cas/",
        secure=True,
    )
    session.cookies.set_cookie(cookie)

    return session


def create_session():
    if config.CAS_USE_COOKIE:
        castgc = resolve_potentially_callable(config.CAS_COOKIE_CASTGC)
        return _create_session_from_cookie(castgc)
    else:
        netId = resolve_potentially_callable(config.CAS_CREDENTIAL_NETID)
        password = resolve_potentially_callable(config.CAS_CREDENTIAL_PASSWORD)
        return _create_session_from_credentials(netId, password)


if __name__ == "__main__":
    # Create a session twice to test resolution.
    session = create_session()
    print(session)
    session = create_session()
    print(session)
    print("Cookies: ", session.cookies.get_dict())
