# mu-ci-ipc

<p align="center">
  <a href="https://hook.lma1980.duckdns.org/jobs">
    <img src="https://hook.lma1980.duckdns.org/cards/LMA1980/mu-ci-ipc/card.svg" alt="mu-ci-ipc Factory Scorecard" width="520"/>
  </a>
</p>

<p align="center">
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/mu-ci-ipc/status.svg" alt="CI Status"/></a>
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/mu-ci-ipc/score.svg" alt="Quality Score"/></a>
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/mu-ci-ipc/coverage.svg" alt="Coverage"/></a>
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/mu-ci-ipc/benchmark.svg" alt="Benchmark"/></a>
</p>

______________________________________________________________________

Zero-copy inter-process communication (IPC) and distributed fabric protocol crate for the [`mu-ci`](https://github.com/LMA1980/mu-ci) ecosystem, shared between [`mu-ci-controller`](https://github.com/LMA1980/mu-ci-controller), [`mu-ci-worker`](https://github.com/LMA1980/mu-ci-worker), and [`mu-ci-isolation`](https://github.com/LMA1980/mu-ci-isolation).

## Core Capabilities

- **Wire Protocol Framing (`zerocopy`)**: Aligned 24-byte C-representation header (`IpcHeader`, magic `b"K3CI"`, version `1`, max frame `16 MiB`) with zero-copy buffer slicing via `zerocopy::FromBytes` and `IntoBytes` (`src/framing.rs`).
- **Control Plane Contracts (`Apache Fory`)**: Schema-evolvable binary serialization (`create_fory_runtime` in `src/control.rs`) for worker registration (including `rust_semver`), heartbeats, stage lifecycle events, privilege-ceiling negotiation, sandbox constraints, typed task invocations, and compiler cache policy (`SccacheMode`, `FORY_ID_SCCACHE_MODE = 15`).
- **Data Plane Streaming (`Apache Arrow`)**: Columnar IPC batch codecs (`src/arrow_stream.rs`) for streaming stage logs, aggregate metrics, granular per-test outcomes, and benchmark runs via `arrow-ipc` `StreamWriter` and `StreamReader`.

## Wire Framing & `msg_type` Discriminants (`src/framing.rs`)

```text
+-----------------------+----------------------------------------------------+
| IpcHeader (24 Bytes)  | Payload (Variable Length)                          |
+-----------------------+----------------------------------------------------+
| magic: [u8; 4]        | b"K3CI"                                            |
| version: u16          | 1 (CURRENT_VERSION)                                |
| msg_type: u16         | 0x01..=0x09 (Fory), 0x10..=0x13 (Arrow IPC)        |
| payload_len: u32      | Payload length in bytes (<= 16 MiB)                |
| flags: u32            | FLAG_COMPRESSED (0x01), FLAG_END_OF_STREAM (0x02)  |
| sequence: u64         | Monotonic frame sequence number                    |
+-----------------------+----------------------------------------------------+
```

| `msg_type` | Constant                       | Encoding    | Payload Type                                                                 |
| :--------- | :----------------------------- | :---------- | :--------------------------------------------------------------------------- |
| `0x01`     | `MSG_TYPE_WORKER_REGISTRATION` | Apache Fory | `WorkerRegistration` (includes `rust_semver: Option<String>`)                |
| `0x02`     | `MSG_TYPE_WORKER_HEARTBEAT`    | Apache Fory | `WorkerHeartbeat`                                                            |
| `0x03`     | `MSG_TYPE_STAGE_DISPATCH`      | Apache Fory | `StageDispatchTask`                                                          |
| `0x04`     | `MSG_TYPE_STAGE_COMPLETION`    | Apache Fory | `StageCompletionReport`                                                      |
| `0x05`     | `MSG_TYPE_STAGE_CANCEL`        | Apache Fory | `StageCancelTask`                                                            |
| `0x06`     | `MSG_TYPE_WORKER_SHUTDOWN`     | Apache Fory | `WorkerShutdown`                                                             |
| `0x07`     | `MSG_TYPE_STAGE_STARTED`       | Apache Fory | `StageStarted`                                                               |
| `0x08`     | `MSG_TYPE_CEILING_REQ`         | Apache Fory | `HostPrivilegeCeilingRequest`                                                |
| `0x09`     | `MSG_TYPE_CEILING_RESP`        | Apache Fory | `HostPrivilegeCeilingResponse`                                               |
| `0x10`     | `MSG_TYPE_ARROW_LOG`           | Arrow IPC   | `LogEntry` (`encode_log_batch` / `decode_log_batch`)                         |
| `0x11`     | `MSG_TYPE_ARROW_METRIC`        | Arrow IPC   | `MetricEntry` (`encode_metric_batch` / `decode_metric_batch`)                |
| `0x12`     | `MSG_TYPE_ARROW_TEST_RUNS`     | Arrow IPC   | `TestCaseRunRecord` (`encode_test_runs_batch` / `decode_test_runs_batch`)    |
| `0x13`     | `MSG_TYPE_ARROW_BENCH_RUNS`    | Arrow IPC   | `BenchmarkRunRecord` (`encode_bench_runs_batch` / `decode_bench_runs_batch`) |

## Fory Control-Plane Type Registry (`src/control.rs`)

| Fory ID | Constant                          | Rust Type                      |
| :------ | :-------------------------------- | :----------------------------- |
| `1`     | `FORY_ID_REGISTRATION`            | `WorkerRegistration`           |
| `2`     | `FORY_ID_HEARTBEAT`               | `WorkerHeartbeat`              |
| `3`     | `FORY_ID_DISPATCH`                | `StageDispatchTask`            |
| `4`     | `FORY_ID_COMPLETION`              | `StageCompletionReport`        |
| `5`     | `FORY_ID_CANCEL`                  | `StageCancelTask`              |
| `6`     | `FORY_ID_WORKER_SHUTDOWN`         | `WorkerShutdown`               |
| `7`     | `FORY_ID_STAGE_STARTED`           | `StageStarted`                 |
| `8`     | `FORY_ID_CEILING_REQ`             | `HostPrivilegeCeilingRequest`  |
| `9`     | `FORY_ID_CEILING_RESP`            | `HostPrivilegeCeilingResponse` |
| `10`    | `FORY_ID_SANDBOX_BACKEND_KIND`    | `SandboxBackendKind`           |
| `11`    | `FORY_ID_NETWORK_EGRESS_MODE`     | `NetworkEgressMode`            |
| `12`    | `FORY_ID_SANDBOX_CONSTRAINT_SPEC` | `SandboxConstraintSpec`        |
| `13`    | `FORY_ID_SECURITY_GRANT_DENIED`   | `SecurityGrantDenied`          |
| `14`    | `FORY_ID_TYPED_TASK_INVOCATION`   | `TypedTaskInvocation`          |
| `15`    | `FORY_ID_SCCACHE_MODE`            | `SccacheMode`                  |

### `SccacheMode` Wire Policy (`FORY_ID_SCCACHE_MODE = 15`)

`StageDispatchTask` and `TypedTaskInvocation` carry `sccache_mode: SccacheMode` across the controller-worker tunnel:

| Variant                        | Accepted YAML / JSON Literals                 | Semantics                                                                                                                                                     |
| :----------------------------- | :-------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SccacheMode::Try` *(default)* | `true`, `"try"`, `"auto"`, `"yes"`, `"1"`     | Use `sccache` for eligible compiler tools when installed and healthy on the worker; fall back cleanly to uncached `rustc` if unavailable.                     |
| `SccacheMode::Mandatory`       | `"mandatory"`, `"required"`, `"strict"`       | Require `sccache`; controller routes compiler stages to `"sccache"`-tagged workers and worker fails closed (`exit 126`) if `sccache` is missing or unhealthy. |
| `SccacheMode::Off`             | `false`, `"off"`, `"disabled"`, `"no"`, `"0"` | Disable `sccache` and strip `RUSTC_WRAPPER` / `CARGO_BUILD_RUSTC_WRAPPER` for the stage or task.                                                              |

## License

Licensed under the GNU Lesser General Public License v3.0 with the LGPL-3.0 Linking Exception (LGPL-3.0-only WITH LGPL-3.0-linking-exception). See [LICENSE](LICENSE) for details.
