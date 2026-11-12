import asyncio
import json
from datetime import datetime

from pydantic import BaseModel

from pipeline.models.sensorthings import SynchronisationPointConfig


class SynchronisationPoint(BaseModel):
    historical_locations: dict[str, datetime]
    observations: dict[str, datetime]


class SensorThingsSynchronisationPoint:
    def __init__(self, config: SynchronisationPointConfig):
        self._file = config.file
        self._storage: SynchronisationPoint = SynchronisationPoint(
            historical_locations={},
            observations={},
        )
        self._last_load_time = None
        self._modified = False

    def set_historical_locations(self, historical_locations: dict[str, datetime]) -> None:
        self._modified = True
        self._storage.historical_locations = historical_locations

    def set_observations(self, observations: dict[str, datetime]) -> None:
        self._modified = True
        self._storage.observations = observations

    async def get_historical_locations(self) -> dict[str, datetime]:
        await self._load_if_required()

        return self._storage.historical_locations

    async def get_observations(self) -> dict[str, datetime]:
        await self._load_if_required()

        return self._storage.observations

    async def store(self):
        def _store():
            if not self._modified:
                return

            if not self._file.parent.exists():
                self._file.parent.mkdir(parents=True, exist_ok=True)

            with self._file.open("w") as f:
                json.dump(self._storage.model_dump_json(), f)
                self._modified = False

        await asyncio.to_thread(_store)

    async def load(self):
        def _load():
            if not self._file.exists():
                return

            with self._file.open("r") as f:
                storage = SynchronisationPoint.model_validate_json(json.load(f))

                self._storage.historical_locations.update(storage.historical_locations)
                self._storage.observations.update(storage.observations)
                self._last_load_time = datetime.now()

        await asyncio.to_thread(_load)

    async def _load_if_required(self):
        if (
            self._last_load_time is None
            or self._last_load_time.timestamp() < self._file.stat().st_mtime
        ):
            await self.load()
