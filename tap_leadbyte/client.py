"""REST client handling, including LeadByteStream base class."""

from __future__ import annotations

import sys
from typing import Any, Callable, Iterable
from urllib.parse import parse_qsl, urlsplit

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources


class LeadByteStream(RESTStream):
    """LeadByte stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        domain = self.config.get("domain", "casesondemand")
        api_version = self.config.get("api_version", "v1.2")
        return f"http://{domain}.leadbyte.com/restapi/{api_version}"

    records_jsonpath = "$.data[*]"  # Default for most endpoints
    next_page_token_jsonpath = "$.next_page"  # May not be needed

    @property
    def authenticator(self) -> None:
        """Return a new authenticator object.
        
        LeadByte uses API key authentication via query parameter.
        """
        return None

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {
            "key": self.config["api_key"],
        }
        
        if next_page_token:
            params.update(next_page_token)
            
        return params

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary for the JSON body of the request.
        """
        # LeadByte API uses GET requests with query parameters
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        resp_json = response.json()
        
        # Check for API errors
        if resp_json.get("status") != "Success":
            self.logger.error(f"API Error: {resp_json.get('message', 'Unknown error')}")
            return
            
        for row in extract_jsonpath(self.records_jsonpath, input=resp_json):
            yield row

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        # LeadByte API doesn't seem to have pagination based on the docs
        return None

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        paginator = self.get_new_paginator()
        decorated_request = self.request_decorator(self._request)

        pages = 0
        with paginator or self._no_pagination_context() as page_context:
            for page_context in page_context:
                resp = decorated_request(
                    prepared_request=self.prepare_request(
                        context=context,
                        next_page_token=page_context,
                    ),
                    context=context,
                )
                pages += 1
                yield from self.parse_response(resp)

        if pages == 0:
            self.logger.info(f"Finished syncing {self.name}. No pages received.")
        else:
            self.logger.info(f"Finished syncing {self.name}. {pages} pages received.")

    def _no_pagination_context(self):
        """Return a context manager for no pagination."""
        return self._NoPaginationContext()

    class _NoPaginationContext:
        """Context manager for no pagination."""
        
        def __enter__(self):
            return [None]
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

