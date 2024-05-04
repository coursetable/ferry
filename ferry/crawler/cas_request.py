"""
Functions for authenticating with Yale Central Authentication System (CAS).

Used by config.py.
"""

import httpx
import asyncio


class RateLimitError(Exception):
    """
    Error object for rate limit exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


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


async def request(
    method: str,
    url: str,
    client: httpx.AsyncClient,
    attempts: int = 1,
    **kwargs,
):
    """
    Helper function to make a request with retries (exponential backoff)

    Parameters
    ----------
    method: str
        HTTP method
    url: str
        URL
    client: AsyncClient
        HTTPX AsyncClient
    attempts: int = 1
        Number of attempts
    **kwargs
        Additional keyword arguments for client.request
    """

    attempt = 0
    response = None

    while response is None and attempt < attempts:
        try:
            response = await client.request(method, url, **kwargs)
            if response.status_code == 429:
                raise RateLimitError()
        except:
            await asyncio.sleep(2**attempt)
            attempt += 1

    if response is None:
        raise ValueError("Request failed: all attempts exhausted.")
    return response
