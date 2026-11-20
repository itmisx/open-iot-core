<div align="center">

# open-iot-core

**A self-hosted MQTT broker and device management plane, wire- and API-compatible with AWS IoT Core.**
Device registry, certificate PKI, Device Shadow, IoT Policy authorization and an admin console are all built in — one static binary, no callback services.

[![license](https://img.shields.io/badge/license-Apache--2.0-blue?logo=apache&logoColor=white)](LICENSE) [![go](https://img.shields.io/badge/go-1.24-00ADD8?logo=go&logoColor=white)](go.mod) [![stage](https://img.shields.io/badge/stage-v0.1%20usable%20core-orange?logo=semver&logoColor=white)](#-known-limitations) [![deps](https://img.shields.io/badge/direct%20deps-3-brightgreen?logo=dependabot&logoColor=white)](#third-party) [![image](https://img.shields.io/badge/ghcr.io-open--iot--core-2496ED?logo=docker&logoColor=white)](https://github.com/itmisx/open-iot-core/pkgs/container/open-iot-core)[![ci](https://img.shields.io/github/actions/workflow/status/itmisx/open-iot-core/ci.yml?branch=main&logo=githubactions&logoColor=white&label=ci)](https://github.com/itmisx/open-iot-core/actions/workflows/ci.yml)

[简体中文](README.md) · **English**

<sub>Not affiliated with, endorsed by, or sponsored by Amazon Web Services.</sub>

</div>

> [!IMPORTANT]
> The current stage is a **v0.1 usable core**: it runs real devices, issues and revokes
> certificates, and exposes operational state. This is not v1.0 and there are **no
> production deployments**. Read [🚨 Known limitations](#-known-limitations) before
> deploying — that section lists existing constraints you will face today, not planned work.

---

## Contents

[✨ Highlights](#-highlights) · [🚀 Quick start](#-quick-start) · [📐 Architecture](#-architecture) · [📦 Capability matrix](#-capability-matrix) · [🚨 Known limitations](#-known-limitations)
[💻 CLI](#-cli) · [🐳 Deployment](#-deployment) · [🔒 Security](#-security) · [📚 Documentation](#-documentation) · [🧪 Development](#-development) · [📄 License and attribution](#-license-and-attribution)

---

## ✨ Highlights

| | |
| --- | --- |
| 📦 **Single binary** | No CGO; the console is compiled into the same file via `go:embed`. No Redis, PostgreSQL or rule-engine sidecar |
| 🔌 **Reuse existing SDKs** | `boto3`, the `aws` CLI and the AWS IoT Device SDK work by pointing their endpoint at this node — no client code changes |
| 🔐 **Secure by default** | Mandatory TLS, mandatory authentication, fail-closed for new connections while the view is stale. Insecure options require three independent acknowledgement gates |
| 📖 **Specification first** | QoS 2, `sensor/#` matching `sensor`, retained delivery to wildcard subscriptions, the DUP flag — where AWS does not support these or deviates from the specification, this project follows OASIS |
| 🧩 **Three direct dependencies** | `bbolt`, `yaml/v3`, `dragonboat`. The console has only `react` and `react-dom` |
| 🪪 **Apache-2.0** | Not BSL / SSPL / ELv2; clustering is not charged separately |
| 🔍 **Explainable authorization** | A policy dry-run returns the decision, the index of the matching statement, the resolved value of every policy variable, and a remediation suggestion — verifiable without connecting a real device |

## 🚀 Quick start

```bash
# 1. Build (the console bundle is committed; Node is not required)
go build -o openiotd ./cmd/openiotd

# 2. Initialize the data directory: CA, server certificate, sample policy, admin token
./openiotd init --data-dir ./data

# 3. Issue and register one device certificate
./openiotd cert issue --data-dir ./data --thing dev-001 --policy device-scoped

# 4. Start
./openiotd serve --data-dir ./data
```

The console is at **<http://127.0.0.1:9090/>**; the first screen requires the token from `./data/admin-token`.

> [!TIP]
> For a pre-populated demo environment, `make demo` creates the CA, issues certificates,
> writes sample data and starts the service; `make demo-clean` removes it. With Docker,
> `deploy/standalone/compose/` is a single `docker compose up -d`; to skip the separate
> `init` step with a bare `docker run`, add `-e OPENIOTD_AUTO_INIT=1` (suitable for
> trials only — see [`deploy/README.md`](deploy/README.md): with it on, mounting the
> wrong path no longer fails, it silently initialises an empty node).

<details>
<summary><b>🧯 Common causes of a failed first connection</b> — check here first when you get <code>NotAuthorized</code></summary>

<br>

The `device-scoped` policy written by `init` allows exactly one prefix, scoped to the device name:

```
Connect     clientId must equal the device name
Publish     $aws/things/<device-name>/*
Subscribe   $aws/things/<device-name>/*
```

Two usages are therefore rejected — **this matches AWS behaviour and is not a fault**:

- a random `clientId` (the default in most GUI clients);
- subscribing to a flat topic such as `sensors/temp` — a flat topic cannot be scoped per device, any device could read and write the same topic, and policy variables would lose their meaning.

The real cause is recorded in the server log's "connection denied" / "authorization denied"
lines (with action, resource, decision, the matching statement index and the resolved value
of every policy variable). It can also be verified through the console's **policy debugger**,
without connecting a real device.

</details>

<details>
<summary><b>🔧 Pointing <code>boto3</code> and the <code>aws</code> CLI at this node</b></summary>

<br>

`endpoint_url` **must include the `/api/aws` prefix**. Without it the request lands on the
console index page and returns HTML; the error reads `invalid character '<'`, which does not
indicate the real cause.

The region must be in the node's allow-list; credentials are access keys issued from the
console's "Access control" page. The full procedure is in the **tutorial** inside the console
(global actions, right of the title bar); its source is under `docs/tutorial/`.

</details>

## 📐 Architecture

```mermaid
flowchart LR
  subgraph Clients
    DEV["Devices<br/>mTLS 8883 · ALPN 443 · WSS"]
    SDK["boto3 / aws CLI<br/>SigV4"]
    OPS["Console / Admin API"]
  end

  subgraph One openiotd process
    direction TB
    BRK["broker<br/>connections and sessions"] --> RTR["router<br/>subscription tree and delivery"]
    AUTHZ["authz<br/>IoT Policy evaluation"]
    SHDW["shadow<br/>Device Shadow"]
    PKI["pki<br/>CA · issuance · revocation"]
    META["meta.Store.Apply<br/>sole metadata write path"]
    BUS["events<br/>single event source"]
  end

  RAFT["raft replication group<br/>dragonboat"]
  PEER["Other nodes<br/>mesh · same TLS connection"]

  DEV --> BRK
  SDK --> AUTHZ
  OPS --> AUTHZ
  BRK --> AUTHZ
  AUTHZ --> SHDW & PKI
  SHDW & PKI --> META
  META --> RAFT
  RTR <--> PEER
  RAFT <--> PEER
  BRK --> BUS
  BUS --> OUT["metrics · event stream · audit.log · device timeline"]
```

Several constraints hold throughout: the **sole path for metadata writes is `meta.Store.Apply`**
(cold-layer records and hot-view changes commit in the same replicated log entry);
instrumentation is **concentrated in the event bus alone**, where metrics accumulate *before*
the sampling gate rather than being derived from the event stream; and single-node writes also
go through a one-replica raft group, so standalone and clustered deployments share one code
path. The derivation is in `docs/prd/02-architecture.md`.

## 📦 Capability matrix

> A vague compatibility claim damages credibility more than a missing feature. The table below
> lists **only what is implemented, covered by tests and green in the merge gate**.

| Domain | Contents |
| --- | --- |
| 🔡 **Protocol** | MQTT 3.1.1 and 5.0, full feature set; QoS 0/1/2; retained; will messages (including Will Delay Interval); session expiry; all MQTT 5 properties; shared subscriptions `$share/` |
| 🔌 **Endpoints** | 8883 mTLS (enabled by default); 443 ALPN `x-amzn-mqtt-ca`; 443 WSS `/mqtt` + SigV4 (the latter two are disabled by default and cannot bind the same address) |
| 🔐 **Authn / authz** | X.509 client certificates; IoT Policy with all policy variables; SigV4 (control plane `iot`, data plane `iotdata`, WebSocket `iotdevicegateway`) |
| 🪪 **PKI** | Built-in CA issuance, CSR issuance, registering your own CA, certificate state machine, disconnecting live connections on revocation |
| 👥 **Platform** | Device Shadow (classic and named, with per-leaf metadata timestamps and 409 version conflicts) |
| 📇 **Registry** | Thing CRUD; certificate-to-device association; search (disabled by default) |
| 🤝 **Compatibility surface** | Control-plane and data-plane actions under `/api/aws`; usable by pointing the `aws` CLI and `boto3` at this node |
| 🛠️ **Admin plane** | `/api/admin/v1`: listing and kicking connections, SSE event stream, policy dry-run, device timeline, shadow read/write, access-key issuance, cluster membership changes |
| 🖥️ **Console** | Overview, devices, certificates, connections, policies, event stream, shadow, MQTT test client and capture, access control, tutorial; Chinese and English; light and dark themes |
| 📊 **Observability** | Prometheus `/metrics` (self-implemented text format); OTLP/HTTP JSON tracing (disabled by default); `audit.log`; `events.jsonl` device timeline |
| 🌐 **Clustering** | Metadata replicated by quorum through dragonboat; cross-node message mesh; cross-node shadow consistency; **persistent sessions and offline queues replicated through session partition groups**; retained messages eventually consistent over the mesh broadcast; admin fan-out; membership changes without restart; cross-node shared subscription groups |
| 🏗️ **Build** | No CGO; cross-compiles for linux/amd64, linux/arm64, darwin/arm64; the console is embedded in the same binary via `go:embed` |

<details>
<summary><b>⚖️ Deliberate differences from AWS</b> — where porting deviations concentrate, listed individually</summary>

<br>

| Item | AWS behaviour | This project |
| --- | --- | --- |
| QoS 2 | Unsupported; no PUBACK/SUBACK when QoS 2 is requested | Full PUBREC/PUBREL/PUBCOMP |
| Does `sensor/#` match `sensor` | No (contrary to the MQTT requirement that `#` match the parent level) | Matches, per the OASIS specification |
| Retained delivery to wildcard subscriptions | Unsupported; the topic must match exactly | Delivered, per the specification |
| DUP flag | Unsupported; redelivery may change the packet ID and does not set DUP | Sets DUP and preserves the packet ID, per the specification |
| MQTT 5 server redirect `0x9C`/`0x9D` | Unsupported | Supported. This is the only graceful mechanism for cluster rebalancing and rolling upgrades |
| Disconnecting live connections on revocation | Immediate disconnection not guaranteed | Disconnected, **bounded by `max_view_staleness`** |
| Message ordering | Message and ACK ordering not guaranteed | Ordered for one publisher to one topic within a node; batch-ordered across nodes |
| Policy dry-run and explanation | None; only after-the-fact log inspection | Returns the decision, the matching statement index, the resolved value of every policy variable, and a remediation suggestion |
| `DescribeEndpoint` | Returns an ATS domain | Returns this node's configured endpoint; `endpointType` is accepted but semantically local |
| Default quotas | The vendor's commercial tiers | Defaults aligned with AWS so behaviour stays predictable |
| Certificate-to-device association | Many-to-many | **One-to-one**; `AttachThingPrincipal` overwrites the previous binding. The authorization path uses it to resolve `${iot:Connection.Thing.ThingName}`, and a policy variable can only hold one value |

</details>

<details>
<summary><b>🚫 Explicitly out of scope</b> — not planned for any release</summary>

<br>

| Excluded | Alternative |
| --- | --- |
| Edge runtimes such as Greengrass | The single binary itself can be deployed as an edge node |
| IoT Analytics / Events / SiteWise / TwinMaker / FleetWise | Separate products; analytics belongs to your own systems |
| IoT Wireless (LoRaWAN / Sidewalk) | None |
| Device Defender ML anomaly detection | Metrics sinks are covered under observability; detection belongs to your own systems |
| Secure Tunneling | `$aws/things/<device-name>/tunnels/notify` is routed as an ordinary topic — no error, and no tunnel semantics |
| Device Location | None |
| Device Advisor / Fleet Hub | Fleet Hub's capabilities are covered by this project's console |
| SiteWise asset-model topics | None |
| Cognito, full IAM, STS | Only SigV4 verification is compatible; the identity model is a local IAM-lite |
| AWS ecosystem rule actions (lambda, dynamodb, kinesis, firehose, sns, sqs, step-functions, timestream, opensearch, cloudwatch, iot-analytics, iot-events, iot-sitewise, salesforce, location) | These actions are not provided |
| Commands (`$aws/commands/...`) | Not implemented |
| CloudTrail, Resource Groups, tag-based billing APIs | Local structured audit log |
| Device protocol gateways other than MQTT (CoAP, LwM2M, MQTT-SN, STOMP) | Not provided. AWS IoT Core itself supports only MQTT and HTTPS |
| MQTT over QUIC | A non-standard extension. Available for inter-node transport, not exposed to devices |
| Bridging to other MQTT brokers | Not offered as a core feature |
| Per-node connection density comparable to EMQX | Targeting 100k–500k connections per node; larger scale relies on horizontal scaling and offloading TLS termination |

</details>

## 🚨 Known limitations

> [!WARNING]
> The following are not planned work, but **existing constraints a deployment faces today**.

**Data and consistency**

- **While a node is down, messages for the offline devices that were last attached to it are no longer queued.** Sessions and offline queues themselves are replicated across nodes (2026-11-20), so a node failure does not lose them; but enqueueing requires the subscription to be in the local routing table, and that only holds on the node the device last attached to. Normal behaviour resumes once that node is back, or once the device reconnects elsewhere (MQTT-128, CLU-111).
- **Cross-node retained-message sync is eventually consistent.** It goes over the mesh broadcast rather than through consensus (2026-11-19); the window is one broadcast, so a subscriber on node B that subscribes before the broadcast arrives sees the old value or nothing. If a broadcast is lost and the topic is never published again, that node stays short of it (watch the `retained_broadcast_dropped` metric).
- **Cross-node read-after-write visibility costs one replication round trip.** Followers apply after commit, asynchronously.
- **Cross-node exclusivity for one clientId is eventually consistent**, with a window of one mesh round trip rather than zero. A strict zero window would require waiting for consensus on the connection path, and that path may not wait on the network.

**Availability**

- **Revocation propagates within an upper bound of `max_view_staleness`** (30 seconds by default; 0 when standalone), not "immediately". A node that has not caught up past that threshold is fail-closed for **new connections**; established connections are not dropped.
- **Issuance and revocation are unavailable while quorum is lost.** This is an inherent trade-off of embedded consensus. Device connections, subscriptions and traffic are unaffected until the staleness threshold above is reached.
- **Quorum is `members/2+1`.** Three nodes tolerate one failure; four nodes also tolerate only one, so growing to an even count does not improve fault tolerance; tolerating two simultaneous failures requires five.

**Implementation behind the baseline**

- **`aws_strict` has no configuration entry point.** The deviations listed under "Deliberate differences from AWS" cannot currently be reverted to AWS behaviour (PRD `CMP-002`, `CMP-003` require this; the implementation has not caught up).
- **Quotas have no configuration entry point.** Defaults are aligned with AWS but are currently hard-coded and not configurable (PRD `QTA-001` requires full configurability).
- **TLS handshake failures produce no metrics and no logs.** A client certificate not issued by this CA, an ALPN mismatch, a missing SNI, a handshake timeout — none of these leave any record server-side (PRD `OBS-021` is not implemented). MQTT-level connection and authorization denials **are** logged.

**Scope of verification**

- **There is no published throughput or latency baseline**, and neither is a merge gate — numbers that cannot be reproduced will not be published. What is committed is the memory baseline (`docs/baseline/memory.json`).
- **The Kubernetes manifests have only been verified on a single-node cluster** (Docker Desktop, v1.34.1): certificates and Secrets, PVC permissions, each Pod taking its own replication certificate, three-node election, follower write forwarding, failover after a hard kill of the leader, **recovery of any deleted Pod with a restart count of 0**, and **persistent sessions and offline queues across Pods (including deleting the Pod that did the enqueueing first)** all passed; the **cross-failure-domain half remains unverified** — a single-node cluster requires removing the anti-affinity constraint to run at all. See [`deploy/cluster/k8s/README.md`](deploy/cluster/k8s/README.md), which also records the conclusion the previous round got wrong and the two defects it hid.

## 💻 CLI

```
openiotd init          Initialize the data directory, generate the CA and server certificate (--if-needed is repeatable)
openiotd serve         Start the node
openiotd cert issue    Issue and register a device certificate
openiotd cert revoke   Revoke a device certificate
openiotd config print  Print the effective configuration after all four layers
openiotd config env    List the available environment variables
openiotd admin token   Print the admin token (--rotate generates and installs a new one)
openiotd version       Print the version
```

Configuration precedence: **defaults < configuration file < environment variables < flags**.
Unknown fields in the configuration file cause startup to fail rather than being ignored.
A sample configuration is in [`examples/openiotd.yaml`](examples/openiotd.yaml).

> [!NOTE]
> `init` and `serve` are two steps, and **`serve` will not initialize the directory**:
> a `serve` that silently creates an empty cluster because `data_dir` was mistyped presents
> as "every device fails authentication", with no way to observe the real cause. In all four
> deployment manifests, init is a preparatory step of the orchestrator and carries
> `--if-needed`, so with an orchestrator it is still a single command.

## 🐳 Deployment

`deploy/` is split into standalone and cluster layers; each manifest states what it provides and what it does **not**:

| Manifest | Description |
| --- | --- |
| [`standalone/compose/`](deploy/standalone/compose/) | Single node, one `docker compose up -d` |
| [`standalone/docker/`](deploy/standalone/docker/) | Single node, plain docker commands, easy to embed in existing scripts |
| [`cluster/compose/`](deploy/cluster/compose/) | Three nodes on one machine, for verifying cluster behaviour — **not** high availability |
| [`cluster/k8s/`](deploy/cluster/k8s/) | Three nodes across machines, **verified only on a single-node cluster** (the cross-failure-domain part is unverified) |

The image is `ghcr.io/itmisx/open-iot-core`, built and pushed by a tag-triggered pipeline for
linux/amd64 and linux/arm64, running as non-root. Build locally with `make image`.

> [!CAUTION]
> Read "One decision to make first" in [`deploy/README.md`](deploy/README.md) before deploying:
> **`pki.server_hosts`** requires re-initialization if
> changed afterwards, and re-initialization replaces the entire trust chain — every certificate
> already issued becomes invalid. Clustering adds two hard constraints (the replication chain is
> independent of the device CA; all three nodes share one device CA), documented in
> [`deploy/cluster/README.md`](deploy/cluster/README.md).

## 🔒 Security

**The default configuration is the secure configuration**: mandatory TLS, mandatory
authentication, fail-closed for new connections while the view is stale. Insecure options must
be enabled explicitly, through three **independent** acknowledgement gates rather than one
combined switch — combining them would let a single acknowledgement take effect on three
unrelated risks:

| Switch | What it acknowledges |
| --- | --- |
| `auth.acknowledge_insecure` | Accepting no authentication at all (`auth.mode: insecure-none`). In that mode, binding the MQTT endpoint to a non-loopback address refuses to start |
| `listeners.admin.acknowledge_public_exposure` | Accepting exposure of the admin plane to the network, even with a token configured |
| `listeners.admin.acknowledge_plaintext_key_export` | Accepting that the issuance API returns device private keys over plaintext HTTP |

The admin plane always requires a Bearer token; there is no "no authentication" option. With no
token configured, `/api/admin/v1` and `/metrics` are **not mounted** at all — requests return
404 rather than a variant answering 401, since the latter would confirm to an attacker that an
admin plane exists here. `/healthz` and `/readyz` are always available and need no token.

**The data directory has no application-level encryption**: the issuing CA private key and the
compatibility-surface access key secrets are both stored in the clear, protected only by `0700` on
the directory and `0600` on the files (`AUTH-092` — the CA passphrase was deliberately removed to
simplify deployment). Leaking the data directory, or any backup of it, is therefore equivalent to
leaking the signing capability of the entire trust chain plus every access key. If you need
encryption at rest, do it **at the volume layer** (dm-crypt, cloud disk encryption, etcd encryption
at rest on k8s).

**One known asymmetry**: `require_sni` defaults to `false`, whereas the PRD (`NET-013`,
`NFR-040`) requires rejecting connections without SNI by default. Self-hosted environments
often have no SNI, but the default and the permissive switch are currently the same value —
this is a deviation between implementation and baseline, not a deliberate trade-off.

🔏 The vulnerability reporting process and response times are in [SECURITY.md](SECURITY.md).
**Please do not describe vulnerability details in a public issue.**

## 📚 Documentation

| Location | Description |
| --- | --- |
| [`docs/prd/`](docs/prd/) | **Requirements baseline**: 18 requirement documents plus a README index, every requirement with a stable ID. Where code conflicts with it, the code is the defect |
| [`docs/TODO.md`](docs/TODO.md) | Progress ledger; records what has been implemented, does not define what should be |
| [`docs/tutorial/`](docs/tutorial/) | Source of the in-console tutorial (parallel Chinese and English trees) |
| [`deploy/README.md`](deploy/README.md) | Required reading before deployment |
| [`docs/baseline/`](docs/baseline/) | Published memory baseline data |
| [`CLAUDE.md`](CLAUDE.md) | Working notes for this repository |

## 🧪 Development

```bash
make gate      # Merge gate; must be entirely green
make test      # Fast regression
make console   # Required after changing web/console/src/ or docs/tutorial/
make sim       # Deep random-seed sweep of the deterministic simulation
make help      # All targets
```

`make gate` = `fmt-check` `vet` `contract` `test-race` `cover-check` `cross` `fuzz-short`.
Coverage thresholds: `internal/mqtt`, `internal/authz` and `internal/shadow` at no less than
**95%**, overall no less than **85%**.

The gate **depends only on the Go toolchain**. Suites that need Python, a JDK, a browser or
network access sit outside it and are run individually:

| Suite | What it verifies | Make target |
| --- | --- | --- |
| [`tests/device-sdk`](tests/device-sdk/) | Whether the real AWS IoT Device SDK can connect and exchange messages | — |
| [`tests/aws-sdk`](tests/aws-sdk/) | Real `boto3` and `aws` CLI against the control and data planes | — |
| [`tests/cluster`](tests/cluster/) | A cluster of three real processes: a hard kill of the leader, membership changes, and **persistent sessions and offline queues across all three nodes** | `make cluster-test` |
| [`tests/console-e2e`](tests/console-e2e/) | One full pass of the console in real Chromium, with no browser errors permitted | `make console-e2e` |
| `tests/examples` | The three SDK examples referenced by the tutorial, run against a temporary node | `make examples-test` |

Before contributing, read the corresponding chapter under `docs/prd/`: an implementation may not
bypass the interface contracts and behavioural boundaries defined there. **Any behavioural
change must update the PRD** (recording decisions settled during implementation in its
"实现期补充" / implementation-addendum subsection) **and `docs/TODO.md`**.

## 📄 License and attribution

This project is licensed under the **Apache License 2.0**; see [LICENSE](LICENSE) for the full
text. It does not use BSL, SSPL, ELv2 or any other source-available license, and clustering is
not charged separately.

Contributions use the **DCO** (Developer Certificate of Origin) rather than a CLA. Include a
`Signed-off-by` line when committing:

```bash
git commit -s -m "..."
```

### Third-party

There are only three direct runtime dependencies, all pure Go and free of CGO:

| Dependency | Purpose | License |
| --- | --- | --- |
| [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt) | Local storage engine (metadata cold layer, sessions, retained messages, shadows) | MIT |
| [`go.yaml.in/yaml/v3`](https://github.com/yaml/go-yaml) | Configuration file parsing | Apache-2.0 and MIT |
| [`github.com/lni/dragonboat/v4`](https://github.com/lni/dragonboat) | Multi-node consensus | Apache-2.0 |

`dragonboat` brings in a number of indirect dependencies (its Pebble log store and related
libraries); their licenses can be inspected through `go.mod` / `go.sum`. The console's only
runtime dependencies are `react` and `react-dom`.

The shape of the compatibility API refers to the **botocore** service model
(<https://github.com/boto/botocore>, Apache License 2.0, Copyright Amazon.com, Inc. or its
affiliates). This project does not reproduce AWS documentation; all descriptions of fields and
behaviour are written independently.

### Trademarks

This project is not affiliated with, endorsed by, or sponsored by Amazon Web Services.
"AWS", "Amazon Web Services" and "AWS IoT Core" are trademarks of Amazon.com, Inc. or its
affiliates; they appear here solely to describe compatibility, and are not used as the project
name, a domain name, or a logo element. Describe this project externally as an
**"AWS IoT Core–compatible API"**.
