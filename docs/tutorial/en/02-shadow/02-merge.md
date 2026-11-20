# Merge semantics, versions and deletion

A shadow has only six rules, and for each of them the observable symptom does not point at its cause. This page states the six rules alongside their symptoms.

## An update is a deep merge, not a wholesale replacement

`update` modifies only the keys present in the request; keys not mentioned retain their values. The following update therefore does not remove `mode`:

```json
{ "state": { "desired": { "interval": 60 } } }
```

A caller who reads it as a replacement will submit the complete document every time. That approach works in isolation, but two clients writing concurrently will overwrite each other, since each submits the whole document as it sees it.

## Deleting a field requires an explicit `null`

```json
{ "state": { "desired": { "mode": null } } }
```

`null` deletes the key rather than blanking it. Callers who cannot find the deletion mechanism commonly submit an empty object `{}` instead, which has no effect at all: merging an object with no keys leaves the document unchanged.

## `version` implements optimistic concurrency control

Including `version` asserts "this change is based on that version": if it does not match the current version the request returns 409 and the write does not take effect. Omitting `version` accepts the write unconditionally.

```json
{ "state": { "desired": { "interval": 60 } }, "version": 7 }
```

When two operators push different values concurrently, the party that omitted `version` silently overwrites the other. A 409 means "read before writing"; it is not a fault.

## `delta` is a difference, not the most recent write

The node compares `desired` and `reported` leaf by leaf, and the values that differ constitute the `delta` pushed to the device. Two consequences follow:

- After acting on a delta the device **must report `reported`**, or the delta persists.
- No delta is produced while `desired` and `reported` agree. Pushing a desired state identical to the current value causes no device-side action; this is not message loss.

`update/documents` carries different information: the complete document before and after the update, for auditing and investigation.

## Deletion resets the version

`delete` removes the entire document. A shadow recreated afterwards is versioned from 1 again.

A write carrying a `version` captured before the deletion returns 409. Automation scripts should either omit the version or perform a `get` first to retrieve the current one.

## Document limits

| Limit | Value | Behaviour on excess |
| --- | --- | --- |
| Document size | 8 KB | The entire update is rejected |
| JSON nesting depth | 8 levels | The entire update is rejected |
| Shadow name length | 64 bytes | Rejected |
| `clientToken` length | 64 bytes | Rejected |

A shadow is not a time series store: writing historical samples into `reported` eventually reaches the 8 KB limit, and the observable symptom is "one update failed" rather than any indication that the design is wrong. Historical data belongs in the message stream.

## `clientToken` is echoed verbatim

When a request carries `clientToken`, the `accepted` and `rejected` responses return that value unchanged. Where a single device has multiple requests in flight, this is how responses are matched to requests: there is only one reply topic, and it does not itself distinguish the originator.

## No replies at all: check the policy first

Every shadow topic resides under `$aws/things/<thing>/`. The starter policy already allows that subtree; a hand-written policy that omits part of `shadow/` presents as "publishes succeed, no replies arrive". See [Access policies](#/guide/policy/basics).
