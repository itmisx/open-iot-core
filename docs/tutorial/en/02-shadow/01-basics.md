# Shadow topics and one complete exchange

A shadow is not an endpoint but a set of **topics**. The device side operates entirely over MQTT; the cloud side may use either MQTT or the data plane's HTTP API.

## Topic inventory for a single device

Replace `lamp-01` with the actual thing name. The classic shadow uses the prefix `$aws/things/<thing>/shadow`; a named shadow inserts an additional `name/<shadow>` segment.

| Purpose | Topic |
| --- | --- |
| Request the current document | `$aws/things/lamp-01/shadow/get` |
| Request succeeded | `$aws/things/lamp-01/shadow/get/accepted` |
| Request failed | `$aws/things/lamp-01/shadow/get/rejected` |
| Submit an update | `$aws/things/lamp-01/shadow/update` |
| Update succeeded | `$aws/things/lamp-01/shadow/update/accepted` |
| Update failed | `$aws/things/lamp-01/shadow/update/rejected` |
| Receive the difference | `$aws/things/lamp-01/shadow/update/delta` |
| Complete document before and after | `$aws/things/lamp-01/shadow/update/documents` |
| Delete the entire document | `$aws/things/lamp-01/shadow/delete` |

Request topics (`get`, `update`, `delete`) are published by the **requester**; topics ending in `accepted`, `rejected`, `delta` and `documents` are published by the node and must be subscribed to in advance.

## Minimal exchange on the device side

At startup a device typically performs three steps: subscribe to the reply topics, request the full document once, then wait for deltas.

```text
SUBSCRIBE  $aws/things/lamp-01/shadow/get/accepted
SUBSCRIBE  $aws/things/lamp-01/shadow/update/delta
PUBLISH    $aws/things/lamp-01/shadow/get          {}
```

The payload of `get` may be the empty object `{}`. On receiving `get/accepted` the device applies `state.desired` and reports the values actually in effect:

```json
{
  "state": {
    "reported": { "interval": 30, "mode": "auto" }
  }
}
```

The reporting step is not optional. If the device adjusts itself without reporting, the `delta` persists indefinitely: a delta is by definition the difference between `desired` and `reported`, and `reported` still holds the previous value.

## Subscribed successfully but receiving nothing: check the policy first

This is the most common form of "the connection is healthy but no data arrives", and its cause does not lie in the shadow: `iot:Subscribe` is evaluated against `topicfilter/` while `iot:Receive` is evaluated against `topic/`. When only the subscription is allowed, SUBACK succeeds but no message can be delivered. See [Access policies](#/guide/policy/basics).

## Named shadows

A single device may hold several shadows, each versioned independently, which allows firmware, configuration and runtime state to be managed separately without interference:

| Purpose | Topic prefix |
| --- | --- |
| Classic shadow | `$aws/things/lamp-01/shadow` |
| Named shadow `firmware` | `$aws/things/lamp-01/shadow/name/firmware` |
| List all named shadows | `$aws/things/lamp-01/shadow/list` |

The console's shadow page is the only entry point for named shadows: the shadow block on the thing detail page reads the classic shadow alone, so a device using named shadows cannot be inspected fully there.

## Entry point in the console

The actions column of the thing list links directly to `#/shadow/<thing>`. That page presents `desired`, `reported` and `delta` side by side and supports pushing a desired state in place; the topic strings in the How-to-use drawer on the right are rendered for the current device and the current shadow, so copying them avoids transcription errors.
