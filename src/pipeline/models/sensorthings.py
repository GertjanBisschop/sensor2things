"""Pydantic models representing SensorThings API entities and commands."""

from datetime import datetime
from enum import Enum, StrEnum, auto
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class SynchronisationPointConfig(BaseModel):
    """Configuration for SensorThings synchronisation point."""

    file: Path


class Config(BaseModel):
    """Configuration for SensorThings sink connector."""

    host: str = "frost"
    password: str | None = None
    poll_interval_seconds: int = 60
    port: int = 8080
    protocol: str = "http"
    synchronisation_point: SynchronisationPointConfig | None = None
    username: str | None = None

    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}/FROST-Server/v1.1"


class Order(StrEnum):
    """Ordering options for SensorThings API queries."""

    ASC = "ASC"
    DESC = "DESC"


class OrderBy(BaseModel, frozen=True):
    """Ordering options for SensorThings API queries."""

    criteria: str
    order: Order = Order.DESC


class HistoricalLocation(BaseModel, frozen=True):
    """SensorThings HistoricalLocation entity."""

    location_name: str
    thing_name: str
    time: datetime


class Location(BaseModel, frozen=True):
    """SensorThings Location entity."""

    thing_name: str | None = None
    description: str | None = None
    latitude: float
    longitude: float
    name: str


class Thing(BaseModel, frozen=True):
    """SensorThings Thing entity."""

    id: str
    definition: str | None = None
    description: str | None = None
    location: Location | None = None
    properties: dict[str, Any] = Field(default_factory=dict)


class Sensor(BaseModel, frozen=True):
    """SensorThings Sensor entity."""

    name: str
    description: str | None = None
    encoding_type: str
    metadata: str | None = None
    properties: dict[str, Any]


class ObservedProperty(BaseModel, frozen=True):
    """SensorThings ObservedProperty entity."""

    name: str
    description: str | None = None
    definition: str | None = None
    properties: dict[str, Any]


class UnitOfMeasurement(BaseModel, frozen=True):
    """SensorThings UnitOfMeasurement entity."""

    name: str
    symbol: str
    definition: str


class FeatureOfInterest(BaseModel, frozen=True):
    """SensorThings FeatureOfInterest entity."""

    name: str
    description: str | None = None
    encoding_type: str
    feature: Any
    properties: dict[str, Any]


class Observation(BaseModel, frozen=True):
    """SensorThings Observation entity."""

    name: str
    feature_of_interest: FeatureOfInterest | None = None
    thing_name: str
    property_name: str
    sensor_name: str
    phenomenon_time: datetime
    result: Any
    result_time: datetime | None = None


class UpdateType(Enum):
    """SensorThings Update type."""

    UPSERT_THING = auto()
    UPSERT_LOCATION = auto()
    UPSERT_HISTORICAL_LOCATION = auto()
    UPSERT_SENSOR = auto()
    UPSERT_OBSERVED_PROPERTY = auto()
    CREATE_UNIT_OF_MEASUREMENT = auto()
    CREATE_OBSERVATION = auto()


class Update(BaseModel):
    """A SensorThings Update"""

    type: UpdateType
    payload: (
        HistoricalLocation
        | Location
        | ObservedProperty
        | Observation
        | Sensor
        | Thing
        | UnitOfMeasurement
    )
