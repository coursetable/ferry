"""
Functions for authenticating with Yale Central Authentication System (CAS).

Used by config.py.
"""

import httpx


class CASClient(httpx.AsyncClient):
    def __init__(self, cas_cookie: str):
        limits = httpx.Limits(max_connections=None, keepalive_expiry=None)
        super().__init__(timeout=None, limits=limits)

        self.cas_cookie = cas_cookie
        self.url = "https://2jbbfpryjyp5wax2tpl5whkfdm0osghd.lambda-url.us-east-1.on.aws/"  # proxy lambda function for concurrency requests
        self.headers.update(
            {"auth_header": "123"}
        )  # auth_header for proxy lambda function
        self.chunk_size = 10  # concurrency limit for proxy lambda function


def create_client(
    cas_cookie: str | None = None,
) -> CASClient:
    """
    Create a client using parameters from /ferry/config.py.
    """
    if cas_cookie is None:
        raise ValueError("cas_cookie is required. Please see ferry/utils.py ")

    return CASClient(cas_cookie)
