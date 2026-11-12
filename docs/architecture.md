# Architecture & System Design

The **SignalK-to-SensorThings** service is an asynchronous, modular data integration pipeline that bridges sensor telemetry and OGC metadata. It supports ingesting from [SignalK](https://signalk.org/) servers or [OGC SensorThings API (STA)](https://www.ogc.org/standard/sensorthings/) instances (such as [FROST-Server](https://fraunhoferiosb.github.io/FROST-Server/)), converting data payloads, and dispatching to target SensorThings endpoints.

---

## 1. Architectural Overview

The application is structured as a decoupled **Producer-Consumer Pipeline** orchestrated by the `Orchestrator`. Communication across pipeline stages is mediated through asynchronous queues (`asyncio.Queue`), decoupling ingestion, transformation, and persistence workers.

```mermaid
flowchart LR
    subgraph Sources [Source Connectors]
        SK_SRC[SignalK Source Connector]
        ST_SRC[SensorThings Source Connector]
    end

    subgraph Pipeline [Pipeline Engine]
        direction TB
        Q_SRC[(Source Queue)]
        
        subgraph Convertors [Convertors]
            SK2ST[SignalK-to-STA Convertor]
            ST2ST[STA-to-STA Convertor]
        end
        
        Q_SINK[(Sink Queue)]
    end

    subgraph Sinks [Sink Connectors]
        ST_SINK[SensorThings Sink Connector]
    end

    SK_SRC -->|SKUpdate| Q_SRC
    ST_SRC -->|STUpdate| Q_SRC
    Q_SRC --> SK2ST
    Q_SRC --> ST2ST
    SK2ST -->|STUpdate| Q_SINK
    ST2ST -->|STUpdate| Q_SINK
    Q_SINK --> ST_SINK
```

### Key Architectural Characteristics
- **Asynchronous Concurrency:** Built on Python `asyncio` with isolated coroutines for source ingestion, transformation, and sink dispatching.
- **Dynamic Artifact Registry:** Plug-and-play architecture where sources, convertors, and sinks register via `@register_pipeline_artifact` and are resolved dynamically from configuration.
- **Strict Pydantic Validation:** SignalK and SensorThings domain models and updates are strongly typed and validated using Pydantic V2.
- **Lazy Metadata Discovery:** In the SignalK source, sensor and observed property metadata are fetched lazily from SignalK REST endpoints on first observation and cached.
- **Synchronized Deduplication & Ingestion Points:** The SensorThings sink and source connectors query synchronization points (latest `time` and `phenomenonTime`) to avoid duplicate or outdated insertions.
- **Dynamic Datastream Provisioning:** SensorThings `Datastream` entities are automatically provisioned on demand when new observation combinations are encountered.

---

## 2. Core Components

```mermaid
classDiagram
    class BaseArtifact {
        <<abstract>>
        +name: str
        +run() async
        +stop() async
        +health_check() async bool
    }

    class BaseSourceConnector~ConfigType~ {
        <<abstract>>
        #_queue: asyncio.Queue
        #_config: ConfigType
        #_running: bool
    }

    class BaseConvertor~ConfigType~ {
        <<abstract>>
        #_source_queue: asyncio.Queue
        #_sink_queue: asyncio.Queue
        #_config: ConfigType
        #_running: bool
    }

    class BaseSinkConnector~ConfigType~ {
        <<abstract>>
        #_queue: asyncio.Queue
        #_config: ConfigType
        #_running: bool
    }

    class Orchestrator {
        -source_connector: BaseSourceConnector
        -convertor: BaseConvertor
        -sink_connector: BaseSinkConnector
        -_tasks: list
        +run() async
        +stop() async
    }

    class SignalKConnector {
        -_rest_api: SignalKRestAPI
        -_ws_url: str
        -_known_sensors: set
        -_known_observations: dict
        +run() async
        +health_check() async bool
    }

    class SensorThingsSourceConnector {
        -_service: SensorThingsService
        -_things: list
        -_last_known_location_name: dict
        -_last_known_historical_location_time: dict
        -_last_known_observation_time: dict
        +run() async
        +health_check() async bool
    }

    class SignalKToSensorThingsConvertor {
        -_conversion_handlers: dict
        +run() async
        +health_check() async bool
    }

    class SensorThingsToSensorThingsConvertor {
        +run() async
        +health_check() async bool
    }

    class SensorThingsSinkConnector {
        -_service: SensorThingsService
        -_things: dict
        -_datastreams: dict
        -_units_of_measurement: dict
        -_last_known_location: STLocation
        -_last_known_historical_location_time: dict
        -_last_known_observation_time: dict
        +run() async
        +health_check() async bool
    }

    BaseArtifact <|-- BaseSourceConnector
    BaseArtifact <|-- BaseConvertor
    BaseArtifact <|-- BaseSinkConnector

    BaseSourceConnector <|-- SignalKConnector
    BaseSourceConnector <|-- SensorThingsSourceConnector
    BaseConvertor <|-- SignalKToSensorThingsConvertor
    BaseConvertor <|-- SensorThingsToSensorThingsConvertor
    BaseSinkConnector <|-- SensorThingsSinkConnector

    Orchestrator o-- BaseSourceConnector
    Orchestrator o-- BaseConvertor
    Orchestrator o-- BaseSinkConnector
```

### 2.1. Orchestrator (`pipeline.orchestrator`)
The `Orchestrator` manages component lifecycles:
1. Loads the validated `Config` model.
2. Resolves registered components from `pipeline.registry` (`SOURCES`, `CONVERTORS`, `SINKS`).
3. Instantiates `source_queue` and `sink_queue` (`asyncio.Queue`).
4. Spawns tasks for the source connector, convertor, and sink connector with `asyncio.create_task`.
5. Monitors execution with `asyncio.gather` and coordinates clean shutdown upon cancellation or SIGINT.

### 2.2. SignalK Source Connector (`pipeline.source_connectors.signalk`)
Ingests data from SignalK using a hybrid REST + WebSocket strategy:
- **Authentication & Vessel Identity:**
  - Obtains a JWT bearer token via `/auth/login`.
  - Queries `/api/self`, `/api/vessels/self/name`, and `/api/vessels/self/navigation/position`.
  - Emits `SKUpdate(type=VESSEL, payload=SKVessel)` to `source_queue`.
- **WebSocket Streaming:**
  - Establishes connection to `/signalk/v1/stream?subscribe=none&token=...`.
  - Subscribes to configured paths (e.g. `environment.*`, `navigation.position`).
  - Filters messages exceeding `max_msg_age_minutes`.
- **Lazy Metadata Ingestion:**
  - For unseen sensor sources, queries `/api/sources/<label>/<src>` and emits `SKUpdate(type=SENSOR_METADATA, payload=SKSensor)`.
  - For unseen observation paths, queries `/api/vessels/self/<path>` and emits `SKUpdate(type=OBSERVATION_METADATA, payload=SKObservedProperty)`.
  - Emits `SKUpdate(type=DELTA, payload=SKDelta)`.

### 2.3. SensorThings Source Connector (`pipeline.source_connectors.sensorthings`)
Polls an existing SensorThings instance (e.g. onboard server) to replicate entities to another STA server (e.g. onshore):
- Syncs initial state: Things, HistoricalLocations, Locations, Sensors, ObservedProperties, and Observations.
- Periodically polls for updates every `poll_interval_seconds` using OData filter queries (`time gt ...`, `phenomenonTime gt ...`).
- Emits structured `STUpdate` commands into `source_queue`.

### 2.4. SignalK to SensorThings Convertor (`pipeline.convertors.signalk_to_sensorthings`)
Transforms `SKUpdate` domain objects into SensorThings `STUpdate` entities:
- **`SKUpdateType.VESSEL`** $\rightarrow$ `STUpdateType.UPSERT_THING` (`STThing` with embedded initial `STLocation`).
- **`SKUpdateType.SENSOR_METADATA`** $\rightarrow$ `STUpdateType.UPSERT_SENSOR` (`STSensor`).
- **`SKUpdateType.OBSERVATION_METADATA`** $\rightarrow$ `STUpdateType.UPSERT_OBSERVED_PROPERTY` (`STObservedProperty`) and `STUpdateType.CREATE_UNIT_OF_MEASUREMENT` (`STUnitOfMeasurement`).
- **`SKUpdateType.DELTA`:**
  - Navigation coordinates (`navigation.position`) $\rightarrow$ `STUpdateType.UPSERT_LOCATION` (`STLocation`).
  - Environmental observations (`environment.*`) $\rightarrow$ `STUpdateType.CREATE_OBSERVATION` (`STObservation`).

### 2.5. SensorThings to SensorThings Convertor (`pipeline.convertors.sensorthings_to_sensorthings`)
Acts as a high-performance asynchronous bridge passing `STUpdate` messages directly from `source_queue` to `sink_queue`.

### 2.6. SensorThings Sink Connector (`pipeline.sink_connectors.sensorthings`)
Synchronizes `STUpdate` commands with the destination SensorThings API:
- **`SensorThingsService` Helper:** Wraps `frost-sta-client` in `asyncio.to_thread` executions to prevent blocking the event loop.
- **Synchronization Point Initialization:** Queries destination STA for the latest `time` on HistoricalLocations and latest `phenomenonTime` on Observations to avoid re-inserting existing telemetry.
- **Entity Ingestion:**
  - `UPSERT_THING`: Upserts the vessel `Thing` and its location.
  - `UPSERT_LOCATION`: Upserts `Location` and updates `_last_known_location`.
  - `UPSERT_HISTORICAL_LOCATION`: Upserts `HistoricalLocation` linked to `Thing` and `Location`.
  - `UPSERT_SENSOR`: Upserts `Sensor`.
  - `UPSERT_OBSERVED_PROPERTY`: Upserts `ObservedProperty`.
  - `CREATE_UNIT_OF_MEASUREMENT`: Caches unit in memory for datastream creation.
  - `CREATE_OBSERVATION`: Ensures `Datastream` exists (provisioning on demand if absent), creates an approximated `FeatureOfInterest` from `_last_known_location`, and persists the `Observation`.

---

## 3. End-to-End Ingestion Sequences

### 3.1. SignalK to SensorThings Ingestion

```mermaid
sequenceDiagram
    autonumber
    actor CLI as pipeline CLI
    participant Orch as Orchestrator
    participant Src as SignalKConnector
    participant REST as SignalK REST API
    participant WS as SignalK WebSocket
    participant Q_Src as source_queue
    participant Conv as SK2ST Convertor
    participant Q_Sink as sink_queue
    participant Sink as SensorThingsConnector
    participant STA as FROST-Server (STA)

    CLI->>Orch: run(config)
    Orch->>Src: run()
    Orch->>Conv: run()
    Orch->>Sink: run()

    %% Startup Handshake
    Note over Src,REST: Initialization & Vessel Discovery
    Src->>REST: POST /auth/login (credentials)
    REST-->>Src: JWT token
    Src->>REST: GET /api/self
    REST-->>Src: vessel identity
    Src->>REST: GET /api/vessels/self/name & position
    REST-->>Src: vessel name & position
    Src->>Q_Src: put(SKUpdate: VESSEL)
    Q_Src->>Conv: get(SKUpdate: VESSEL)
    Conv->>Q_Sink: put(STUpdate: UPSERT_THING)
    Q_Sink->>Sink: get(STUpdate: UPSERT_THING)
    Sink->>STA: Upsert Thing & Location

    %% WebSocket streaming
    Note over Src,WS: WebSocket Stream Connection
    Src->>WS: Connect (token)
    WS-->>Src: Hello Message
    Src->>WS: Unsubscribe (*)
    Src->>WS: Subscribe (vessels.self, environment.*, navigation.position)

    %% Delta Ingestion
    loop Continuous Streaming
        WS-->>Src: Delta Message (JSON)
        opt Unseen Sensor
            Src->>REST: GET /api/sources/<label>/<source>
            REST-->>Src: Sensor Metadata
            Src->>Q_Src: put(SKUpdate: SENSOR_METADATA)
            Q_Src->>Conv: get(SKUpdate: SENSOR_METADATA)
            Conv->>Q_Sink: put(STUpdate: UPSERT_SENSOR)
            Q_Sink->>Sink: get(STUpdate: UPSERT_SENSOR)
            Sink->>STA: Upsert Sensor
        end

        opt Unseen Observed Property
            Src->>REST: GET /api/vessels/self/<path>
            REST-->>Src: Property Metadata
            Src->>Q_Src: put(SKUpdate: OBSERVATION_METADATA)
            Q_Src->>Conv: get(SKUpdate: OBSERVATION_METADATA)
            Conv->>Q_Sink: put(STUpdate: UPSERT_OBSERVED_PROPERTY & CREATE_UNIT_OF_MEASUREMENT)
            Q_Sink->>Sink: get(STUpdate: UPSERT_OBSERVED_PROPERTY)
            Sink->>STA: Upsert ObservedProperty
        end

        Src->>Q_Src: put(SKUpdate: DELTA)
        Q_Src->>Conv: get(SKUpdate: DELTA)
        alt Navigation Update
            Conv->>Q_Sink: put(STUpdate: UPSERT_LOCATION)
            Q_Sink->>Sink: get(STUpdate: UPSERT_LOCATION)
            Sink->>STA: Upsert Location & update _last_known_location
        else Environmental Metric Update
            Conv->>Q_Sink: put(STUpdate: CREATE_OBSERVATION)
            Q_Sink->>Sink: get(STUpdate: CREATE_OBSERVATION)
            opt Datastream not yet created
                Sink->>STA: Create Datastream (Thing + Sensor + ObservedProperty)
            end
            Sink->>STA: Create Observation (with FeatureOfInterest)
        end
    end
```

### 3.2. SensorThings to SensorThings Ingestion

```mermaid
sequenceDiagram
    autonumber
    participant Src as ST Source Connector
    participant SourceSTA as Source FROST-Server
    participant Q_Src as source_queue
    participant Conv as ST2ST Convertor
    participant Q_Sink as sink_queue
    participant Sink as ST Sink Connector
    participant DestSTA as Destination FROST-Server

    Note over Src,SourceSTA: Initial Synchronization
    Src->>SourceSTA: GET /Things, /Sensors, /ObservedProperties, /Datastreams
    SourceSTA-->>Src: Entity Lists
    Src->>Q_Src: put(STUpdate: UPSERT_*)
    Q_Src->>Conv: get(STUpdate)
    Conv->>Q_Sink: put(STUpdate)
    Q_Sink->>Sink: get(STUpdate)
    Sink->>DestSTA: Upsert Entities

    loop Periodic Polling (poll_interval_seconds)
        Src->>SourceSTA: GET /HistoricalLocations?$filter=time gt <last_time>
        SourceSTA-->>Src: New Historical Locations
        Src->>Q_Src: put(STUpdate: UPSERT_HISTORICAL_LOCATION)
        
        Src->>SourceSTA: GET /Observations?$filter=phenomenonTime gt <last_time>
        SourceSTA-->>Src: New Observations
        Src->>Q_Src: put(STUpdate: CREATE_OBSERVATION)
        
        Q_Src->>Conv: get & forward
        Conv->>Q_Sink: put
        Q_Sink->>Sink: get & dispatch
        Sink->>DestSTA: Upsert New Records
    end
```

---

## 4. SignalK to SensorThings Domain Mapping

```mermaid
erDiagram
    SK_VESSEL ||--o{ STA_THING : "maps to"
    SK_POSITION ||--o{ STA_LOCATION : "maps to"
    SK_POSITION ||--o{ STA_FOI : "approximates as FeatureOfInterest"
    SK_SOURCE ||--o{ STA_SENSOR : "maps to"
    SK_METADATA ||--o{ STA_OBSERVED_PROPERTY : "maps to"
    SK_METADATA ||--o{ STA_UOM : "maps to UnitOfMeasurement"
    SK_DELTA ||--o{ STA_OBSERVATION : "maps to"
    
    STA_THING ||--o{ STA_LOCATION : "has"
    STA_THING ||--o{ STA_HISTORICAL_LOCATION : "has"
    STA_THING ||--o{ STA_DATASTREAM : "owns"
    STA_SENSOR ||--o{ STA_DATASTREAM : "attached to"
    STA_OBSERVED_PROPERTY ||--o{ STA_DATASTREAM : "observed by"
    STA_DATASTREAM ||--o{ STA_OBSERVATION : "contains"
    STA_OBSERVATION ||--o{ STA_FOI : "has"

    SK_VESSEL {
        string id "vessels.urn:mrn:signalk..."
        string name "Vessel Name"
    }

    SK_SOURCE {
        string label "e.g. HangerPrincess"
        string src "e.g. 35 (N2K) or Talker (N0183)"
        string meta "Hardware specs & PGNs"
    }

    SK_METADATA {
        string path "environment.outside.temperature"
        string units "K / degC"
        string description "Outside air temperature"
    }

    SK_DELTA {
        string timestamp "2026-08-25T08:28:30Z"
        string value "298.15"
    }

    STA_THING {
        string id "Vessel Context ID"
        string name "Vessel Name"
        string properties "sk_id, sk_name"
    }

    STA_LOCATION {
        string name "vessel#source#uuid7"
        string coordinates "[lon, lat]"
        string encoding_type "application/geo+json"
    }

    STA_HISTORICAL_LOCATION {
        string time "ISO timestamp"
        string thing "Thing reference"
        string location "Location reference"
    }

    STA_SENSOR {
        string name "context#label.source"
        string description "Sensor located at source"
        string properties "PGNs / Sentences"
    }

    STA_OBSERVED_PROPERTY {
        string name "context#source#path"
        string description "Property description"
    }

    STA_DATASTREAM {
        string name "context#source#path"
        string observation_type "OM_Measurement"
    }

    STA_OBSERVATION {
        string phenomenon_time "Timestamp"
        string result "Measurement value"
    }

    STA_FOI {
        string name "observation_name"
        string feature "[lon, lat]"
    }
```

### Entity Mapping Details

| SignalK Concept | SensorThings Entity | Mapping Strategy & Notes |
| :--- | :--- | :--- |
| **Vessel (`self`)** | `Thing` | The SignalK vessel context ID becomes the `Thing` identifier. Properties contain `{"sk_id": vessel_id, "sk_name": vessel_name}`. |
| **Navigation (`navigation.position`)** | `Location` | GPS coordinates create an `STLocation` named `<vessel_id>#<source>#<uuid7>`. Stored in `_last_known_location` for subsequent observation geospatial references. |
| **Source (`sources/<label>/<src>`)** | `Sensor` | Unique name generated as `<context>#<label>.<src>` (or `<context>#<label>.<talker>` for NMEA 0183). Contains hardware PGNs / sentences in `properties`. |
| **Path Metadata (`<path>/meta`)** | `ObservedProperty` & `UnitOfMeasurement` | Name generated as `<context>#<source>#<path>`. Units stored in `UnitOfMeasurement`, and metadata properties stored in `ObservedProperty.properties`. |
| **Delta Value (`updates.values`)** | `Observation` & `Datastream` | Ingested into a `Datastream` (`<context>#<source>#<path>`). Creates an `Observation` with `phenomenon_time` matching the SignalK timestamp. |

---

## 5. Configuration & Deployment

Configurations are written in YAML or JSON and loaded via `pipeline.models.config.Config`:

### SignalK to SensorThings (`config/sk2st_config.yaml`)
```yaml
log_level: info

source:
  signalk:
    username: readonly
    password: signalk_ro
    subscriptions:
      context: vessels.self
      subscriptions:
        - path: environment.*
          period: 1000
          policy: fixed
          format: delta
        - path: navigation.position
          period: 1000
          policy: fixed
          format: delta

convertor:
  signalk_sensorthings: {}

sink:
  sensorthings:
    host: web
```

### SensorThings to SensorThings (`config/st2st_config.yaml`)
```yaml
log_level: info

source:
  sensorthings:
    host: web

convertor:
  sensorthings_sensorthings: {}

sink:
  sensorthings:
    host: web2
```

### CLI Execution
The application is invoked via the CLI entrypoint registered in `pyproject.toml`:

```bash
# Run default configuration (config.yaml)
uv run pipeline

# Run specific configuration file
uv run pipeline -c config/sk2st_config.yaml
uv run pipeline --config-path config/st2st_config.yaml
```

---

## 6. Project Directory Reference

```
src/pipeline/
├── __init__.py                  # CLI entrypoint, logging setup, run_pipeline()
├── orchestrator.py              # Orchestrator lifecycle & queue management
├── registry.py                  # Dynamic artifact registry (@register_pipeline_artifact)
├── error.py                     # Custom exception hierarchy
├── models/                      # Pydantic data models
│   ├── __init__.py              # Exported domain models & enums
│   ├── artifacts.py             # BaseArtifact, BaseSourceConnector, BaseConvertor, BaseSinkConnector
│   ├── config.py                # Config schema & loader
│   ├── convertor.py             # Convertor configuration models
│   ├── sensorthings.py          # SensorThings internal entities & update commands
│   └── signalk.py               # SignalK delta, meta, position & update models
├── convertors/                  # Convertor implementations
│   ├── __init__.py
│   ├── sensorthings_to_sensorthings.py # SensorThings-to-SensorThings convertor
│   └── signalk_to_sensorthings.py      # SignalK-to-SensorThings convertor
├── source_connectors/           # Source connector implementations
│   ├── __init__.py
│   ├── sensorthings.py          # SensorThings REST polling source connector
│   └── signalk.py               # SignalK WebSocket & REST streaming connector
├── sink_connectors/             # Sink connector implementations
│   ├── __init__.py
│   └── sensorthings.py          # SensorThings sink connector
└── utils/                       # Shared utility services & helpers
    ├── __init__.py
    ├── decorators.py            # Utility decorators (classproperty)
    ├── sensorthings_service.py  # FROST-Server client wrapper
    └── signalk_rest_api.py      # SignalK REST API HTTP client
```
