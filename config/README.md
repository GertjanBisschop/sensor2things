# Configuration

Configuration is specified using YAML or JSON and loaded at runtime via `pipeline.models.config.Config`.

The pipeline architecture is composed of three decoupled stages: **source**, **convertor**, and **sink**. Each stage is configured under its respective top-level key in the configuration file.

---

## Configuration Schema

### Top-Level Settings

| Field       | Type     | Default  | Description                                                            |
| :---------- | :------- | :------- | :--------------------------------------------------------------------- |
| `log_level` | `string` | `"info"` | Logging verbosity: `debug`, `info`, `warning`, `error`, or `critical`. |
| `source`    | `object` | `{}`     | Exactly one source connector configuration block.                      |
| `convertor` | `object` | `{}`     | Exactly one convertor configuration block.                             |
| `sink`      | `object` | `{}`     | Exactly one sink connector configuration block.                        |

---

### Source Connectors (`source`)

#### 1. `signalk` (`SKConfig`)

Connects to a SignalK server via REST (for authentication and metadata discovery) and WebSocket (for delta telemetry streaming).

| Field                 | Type      | Default      | Description                                                     |
| :-------------------- | :-------- | :----------- | :-------------------------------------------------------------- |
| `host`                | `string`  | `"signalk"`  | SignalK server hostname or IP address.                          |
| `port`                | `integer` | `3000`       | SignalK server port.                                            |
| `server_name`         | `string`  | `"signalk"`  | Base path / server identifier in the URL.                       |
| `username`            | `string`  | _(required)_ | Username for SignalK REST API authentication.                   |
| `password`            | `string`  | _(required)_ | Password for SignalK REST API authentication.                   |
| `protocol_rest`       | `string`  | `"http"`     | Protocol used for REST API requests (`http` or `https`).        |
| `protocol_ws`         | `string`  | `"ws"`       | Protocol used for WebSocket streaming (`ws` or `wss`).          |
| `max_msg_age_minutes` | `integer` | `10`         | Discard incoming delta messages older than this age in minutes. |
| `subscriptions`       | `object`  | _see below_  | SignalK WebSocket subscription configuration.                   |

**`subscriptions` block (`SubscriptionsConfig`):**

| Field           | Type           | Default                                                        | Description                             |
| :-------------- | :------------- | :------------------------------------------------------------- | :-------------------------------------- |
| `context`       | `string`       | `"vessels.self"`                                               | Target vessel context.                  |
| `subscriptions` | `list[object]` | `[{"path": "environment.*"}, {"path": "navigation.position"}]` | List of subscription paths and options. |

Each subscription item accepts:

- `path` (`string`, required): SignalK data path pattern (e.g. `environment.*`, `navigation.position`).
- `period` (`integer`, default: `10000`): Ingestion period in milliseconds.
- `policy` (`string`, default: `"fixed"`): Subscription policy (`fixed`, `instant`, etc.).
- `format` (`string`, default: `"delta"`): Message format (`delta` or `full`).

#### 2. `sensorthings` (`STConfig`)

Connects to an OGC SensorThings API server (e.g., FROST-Server) as a source and polls entities periodically.

| Field                   | Type             | Default   | Description                                               |
| :---------------------- | :--------------- | :-------- | :-------------------------------------------------------- |
| `host`                  | `string`         | `"frost"` | SensorThings server hostname or IP address.               |
| `port`                  | `integer`        | `8080`    | SensorThings server port.                                 |
| `protocol`              | `string`         | `"http"`  | Connection protocol (`http` or `https`).                  |
| `username`              | `string \| null` | `null`    | Optional HTTP basic authentication username.              |
| `password`              | `string \| null` | `null`    | Optional HTTP basic authentication password.              |
| `poll_interval_seconds` | `integer`        | `60`      | Polling interval in seconds for synchronizing updates.    |
| `synchronisation_point` | `object \| null` | `null`    | Optional synchronisation point configuration (see below). |

**`synchronisation_point` block (`SynchronisationPointConfig`):**

| Field  | Type     | Default      | Description                                                                   |
| :----- | :------- | :----------- | :---------------------------------------------------------------------------- |
| `file` | `string` | _(required)_ | Path to the JSON file used to persist and restore synchronisation timestamps. |

---

### Convertors (`convertor`)

#### 1. `signalk_sensorthings` (`SK2STConfig`)

Transforms SignalK domain models (`SKUpdate`) into SensorThings entities (`STUpdate`). Requires no extra parameters:

```yaml
convertor:
  signalk_sensorthings: {}
```

#### 2. `sensorthings_sensorthings` (`ST2STConfig`)

Pass-through convertor bridging SensorThings source updates directly to the SensorThings sink queue. Requires no extra parameters:

```yaml
convertor:
  sensorthings_sensorthings: {}
```

---

### Sink Connectors (`sink`)

#### 1. `sensorthings` (`STConfig`)

Persists entities and observations into a destination SensorThings API (FROST-Server) instance.

| Field                   | Type             | Default   | Description                                                                             |
| :---------------------- | :--------------- | :-------- | :-------------------------------------------------------------------------------------- |
| `host`                  | `string`         | `"frost"` | Destination SensorThings server hostname or IP address.                                 |
| `port`                  | `integer`        | `8080`    | Destination SensorThings server port.                                                   |
| `protocol`              | `string`         | `"http"`  | Connection protocol (`http` or `https`).                                                |
| `username`              | `string \| null` | `null`    | Optional HTTP basic authentication username.                                            |
| `password`              | `string \| null` | `null`    | Optional HTTP basic authentication password.                                            |
| `poll_interval_seconds` | `integer`        | `60`      | Polling interval parameter.                                                             |
| `synchronisation_point` | `object \| null` | `null`    | Optional synchronisation point configuration (`SynchronisationPointConfig`, see above). |

---

## Configuration Examples

### 1. SignalK to SensorThings (`config/sk2st_config.yaml`)

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

### 2. SensorThings to SensorThings (`config/st2st_config.yaml`)

```yaml
log_level: info

source:
  sensorthings:
    host: web
    synchronisation_point:
      file: /data/synchronisation_point.json

convertor:
  sensorthings_sensorthings: {}

sink:
  sensorthings:
    host: web2
    synchronisation_point:
      file: /data/synchronisation_point.json
```

---

## Running with Configuration

Run the pipeline by specifying the configuration file path:

```bash
uv run pipeline -c config/sk2st_config.yaml
# or
uv run pipeline --config-path config/st2st_config.yaml
```
