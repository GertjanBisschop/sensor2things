"""Pydantic models for SignalK messages, deltas, and metadata."""

from datetime import datetime
from enum import Enum, StrEnum, auto
from typing import Any, Literal

from pydantic import BaseModel, Field


class SubscriptionDetailsConfig(BaseModel):
    """Configuration for SignalK subscription."""

    format: str = "delta"
    path: str
    period: int = 10000
    policy: str = "fixed"


class SubscriptionsConfig(BaseModel):
    """Configuration for SignalK subscriptions."""

    context: str = "vessels.self"
    subscriptions: list[SubscriptionDetailsConfig] = Field(
        default_factory=lambda: [
            SubscriptionDetailsConfig(path="environment.*"),
            SubscriptionDetailsConfig(path="navigation.position"),
        ]
    )


class Config(BaseModel):
    """Configuration for SignalK source connector."""

    host: str = "signalk"
    max_msg_age_minutes: int = 10
    username: str
    password: str
    port: int = 3000
    server_name: str = "signalk"
    subscriptions: SubscriptionsConfig = SubscriptionsConfig()
    protocol_rest: str = "http"
    protocol_ws: str = "ws"

    @property
    def base_url(self) -> str:
        return f"{self.protocol_rest}://{self.host}:{self.port}/{self.server_name}/v1"

    @property
    def ws_base_url(self) -> str:
        return f"{self.protocol_ws}://{self.host}:{self.port}/{self.server_name}/v1"


class DeltaSourceType(StrEnum):
    NMEA0183 = "NMEA0183"
    NMEA2000 = "NMEA2000"


class DeltaSourceN2000(BaseModel, frozen=True):
    """Metadata about the original sensor / bus source of a delta update."""

    canName: str | None = None
    deviceInstance: int | None = None
    instance: str | None = None
    label: str
    type: Literal[DeltaSourceType.NMEA2000]
    pgn: int
    src: str


class DeltaSourceN0183(BaseModel, frozen=True):
    """Metadata about the original sensor / bus source of a delta update."""

    label: str
    type: Literal[DeltaSourceType.NMEA0183]
    talker: str
    sentence: str


class DeltaValue(BaseModel):
    """A single path/value pair in a SignalK delta update."""

    path: str
    value: Any


class DeltaUpdate(BaseModel, validate_by_name=True, validate_by_alias=True):
    """An update section within a SignalK delta message."""

    source: DeltaSourceN0183 | DeltaSourceN2000
    source_ref: str = Field(validation_alias="$source")
    timestamp: datetime
    values: list[DeltaValue] = Field(default_factory=list)


class Delta(BaseModel):
    """SignalK Delta message payload."""

    context: str
    updates: list[DeltaUpdate] = Field(default_factory=list)


class Hello(BaseModel):
    """SignalK server hello message."""

    name: str | None = None
    roles: list[str] = Field(default_factory=list)
    self: str | None = None
    serverStartId: str | None = None
    timestamp: datetime
    version: str


class Position(BaseModel, frozen=True):
    """Position data for a SignalK vessel."""

    description: str | None = None
    latitude: float
    longitude: float
    source: str


class Vessel(BaseModel, frozen=True):
    """SignalK vessel data."""

    id: str
    name: str | None = None
    position: Position | None = None


class SensorMetaN2000(BaseModel, frozen=True):
    """Additional metadata for a sensor."""

    src: str
    pgns: dict[str, str]


class SensorMetaN0183(BaseModel, frozen=True):
    """Additional metadata for a sensor."""

    talker: str
    sentences: dict[str, str]


class Sensor(BaseModel, frozen=True):
    """Metadata for a sensor."""

    context: str
    name: str
    meta: SensorMetaN0183 | dict[str, SensorMetaN2000] | dict[str, Any]


class ObservedPropertyDisplayUnits(BaseModel, frozen=True):
    """Display units for an observed property."""

    category: str | None
    displayFormat: str | None
    formula: str | None
    inverseFormula: str | None
    symbol: str
    targetUnit: str


class ObservedPropertyMetaSingle(BaseModel, frozen=True):
    """Additional metadata for an observed property."""

    description: str | None
    displayUnits: ObservedPropertyDisplayUnits
    units: str


class ObservedPropertyMetaProperties(BaseModel, frozen=True):
    """Additional metadata for an observed property."""

    description: str | None
    example: Any
    type: str | None
    units: str


class ObservedPropertyMetaMultiple(BaseModel, frozen=True):
    """Additional metadata for an observed property."""

    description: str | None
    properties: dict[str, ObservedPropertyMetaProperties]


class ObservedProperty(BaseModel, frozen=True):
    """Metadata for an observed property."""

    context: str
    source: str
    path: str
    meta: ObservedPropertyMetaSingle | ObservedPropertyMetaMultiple | dict[str, Any]


class UpdateType(Enum):
    """Type of SignalK update."""

    VESSEL_METADATA = auto()
    SENSOR_METADATA = auto()
    OBSERVED_PROPERTY_METADATA = auto()
    DELTA = auto()


class Update(BaseModel):
    """A SignalK update."""

    type: UpdateType
    payload: Delta | ObservedProperty | Sensor | Vessel


# --- Context mapping types ---


class VesselContextKey(BaseModel, frozen=True):
    """Key for looking up vessel context; uniqueness is defined by the vessel id."""

    id: str


class SensorContextKey(BaseModel, frozen=True):
    """Key for looking up sensor context; uniqueness is defined by context and name."""

    context: str
    name: str


class ObservedPropertyContextKey(BaseModel, frozen=True):
    """Key for looking up observed property context; uniqueness is defined by context, source, and path."""

    context: str
    source: str
    path: str


class Context(BaseModel, frozen=True):
    """Base context with a definition URI."""

    definition: str  # URI


class VesselContext(Context, frozen=True):
    """Context for a vessel."""


class SensorContext(Context, frozen=True):
    """Context for a sensor."""


class UnitContext(Context, frozen=True):
    """Context for a unit of measurement."""

    symbol: str


class ObservedPropertyContext(Context, frozen=True):
    """Context for an observed property."""

    unit: UnitContext
