"""
Functions for authenticating with Yale Central Authentication System (CAS).
"""

import asyncio
import os
import time

import httpx
import requests

# User-Agent header to avoid AWS WAF challenges
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"


class RateLimitError(Exception):
    pass


class CASClient(requests.Session):
    def __init__(self, cas_cookie: str):
        super().__init__()
        self.cas_cookie = cas_cookie


def sync_request(
    url: str,
    client: CASClient,
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
    client: Client
        HTTPX Client
    attempts: int = 1
        Number of attempts
    **kwargs
        Additional keyword arguments for client.request
    """

    attempt = 0
    response = None

    while response is None and attempt < attempts:
        try:
            # Non-ideal but works for now
            os.system(
                f'curl -s -f -H "Cookie: {client.cas_cookie}" "{url}" -o /tmp/out'
            )
            with open("/tmp/out", "rb") as file:
                response = file.read()
                if (
                    "The requested URL returned error" in str(response)
                    or len(str(response)) == 0
                ):
                    response = None
        except:
            time.sleep(2**attempt)
            attempt += 1

    if response is None:
        raise ValueError("Request failed: all attempts exhausted.")
    return response


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
