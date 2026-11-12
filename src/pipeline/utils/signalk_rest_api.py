import logging
from typing import Any

import httpx2

from pipeline.error import PipelineError

# isort: off
from pipeline.models.signalk import (
    Config as SKConfig,
    Position as SKPosition,
)
# isort: on

logger = logging.getLogger(__name__)


class SignalKRestAPIError(PipelineError):
    """Error raised when there is an issue with the SignalK REST API."""


class SignalKRestAPI:
    """SignalK REST API client."""

    def __init__(self, config: SKConfig) -> None:
        self._config = config
        self._login_url = f"{config.base_url}/auth/login"
        self._api_url = f"{config.base_url}/api"
        self._token: str | None = None

    async def get_token(self) -> str:
        """Authenticate with SignalK REST API and retrieve JWT token."""
        async with httpx2.AsyncClient() as client:
            return await self._get_token(client)

    async def get_identity(self) -> str:
        """Return the identity of self."""
        url = f"{self._api_url}/self"
        response = await self._api_get(url)

        if not isinstance(response, str):
            raise SignalKRestAPIError(f"Received unexpected reply for {url}: {response}")

        return response

    async def get_name(self) -> str:
        """Return the name of self."""
        url = f"{self._api_url}/vessels/self/name"
        response = await self._api_get(url)

        if not isinstance(response, str):
            raise SignalKRestAPIError(f"Received unexpected reply for {url}: {response}")

        return response

    async def get_position(self) -> SKPosition:
        """Return the position of self."""
        url = f"{self._api_url}/vessels/self/navigation/position"
        response = await self._api_get(url)

        if not isinstance(response, dict):
            raise SignalKRestAPIError(f"Received unexpected reply for {url}: {response}")

        value = response.get("value")
        if value is None or value.get("latitude") is None or value.get("longitude") is None:
            raise SignalKRestAPIError(f"Received unexpected reply for {url}: {response}")

        description = response.get("meta", {}).get("description")
        source = response.get("$source") or ""

        return SKPosition(
            latitude=value["latitude"],
            longitude=value["longitude"],
            description=description,
            source=source,
        )

    async def get_source_metadata(self, label: str, source: str) -> dict:
        """Return the metadata of the specified resource."""
        url = f"{self._api_url}/sources/{label}/{source}"
        response = await self._api_get(url)

        if not isinstance(response, dict):
            raise SignalKRestAPIError(f"Received unexpected reply for {url}: {response}")

        return response

    async def get_observation_metadata(self, observation: str) -> dict:
        """Return the metadata of the specified observation."""
        url = f"{self._api_url}/vessels/self/{observation}"
        response = await self._api_get(url)

        if not isinstance(response, dict):
            raise SignalKRestAPIError(f"Received unexpected reply for {url}: {response}")

        meta = response.get("meta")
        if meta is None or not isinstance(meta, dict):
            raise SignalKRestAPIError(f"Received unexpected reply for {url}: {response}")

        return response["meta"]

    async def _api_get(self, url: str) -> dict[str, Any] | list[Any] | str:
        """Make a GET request to the SignalK REST API."""
        async with httpx2.AsyncClient() as client:
            if self._token is None:
                await self._get_token(client)

            try:
                response = await client.get(url, headers={"Authorization": f"Bearer {self._token}"})
                response.raise_for_status()

                return response.json()
            except httpx2.RequestError as e:
                raise SignalKRestAPIError(f"Failed to retrieve data for {url}: {e}")
            except httpx2.HTTPStatusError as e:
                raise SignalKRestAPIError(f"Received unexpected reply for {url}: {e}")

    async def _get_token(self, client: httpx2.AsyncClient) -> str:
        """Authenticate with SignalK REST API and retrieve JWT token."""
        logger.debug(f"Authenticating with SignalK at: {self._login_url}")
        try:
            response = await client.post(
                self._login_url,
                json={"username": self._config.username, "password": self._config.password},
                timeout=5,
            )
            response.raise_for_status()

            token = response.json().get("token", None)
            if not isinstance(token, str):
                raise SignalKRestAPIError("Failed to authenticate with SignalK")

            self._token = token
            return token
        except httpx2.RequestError as e:
            raise SignalKRestAPIError(
                f"Failed to authenticate with SignalK on {self._login_url}: {e}"
            )
        except httpx2.HTTPStatusError as e:
            raise SignalKRestAPIError(
                f"Unexpected response when authenticating on {self._login_url}: {e}"
            )
