"""
Functions for authenticating with Yale Central Authentication System (CAS).

Used by config.py.
"""

import httpx

from ferry.includes.utils import resolve_potentially_callable


def _create_client_from_cookie(cas_cookie: str) -> httpx.AsyncClient:
    limits = httpx.Limits(max_connections=None, keepalive_expiry=None)
    client = httpx.AsyncClient(timeout=None, limits=limits)

    client.cas_cookie = cas_cookie
    client.url = "https://aa6j3dg4vknlw2zp5hrji4oh540triws.lambda-url.us-east-2.on.aws/"  # proxy lambda function for concurrency requests
    client.headers.update(
        {"auth_header": "123"}
    )  # auth_header for proxy lambda function
    client.chunk_size = 10  # concurrency limit for proxy lambda function

    return client


def create_client(
    cas_cookie: str = None,
) -> httpx.AsyncClient:
    """
    Create a client using parameters from /ferry/config.py.
    """

    if cas_cookie is None:
        raise ValueError("cas_cookie is required. Please see ferry/utils.py ")

    cas_cookie = resolve_potentially_callable(cas_cookie)
    return _create_client_from_cookie(cas_cookie=cas_cookie)
