# Diagnosing a denied request

A denied connection or subscription should not be diagnosed by conjecture. The console's policy debugger exposes the decision itself: which statement matched, what each variable resolved to, and what is still missing for the request to be allowed.

## Using the policy debugger

The entry point is "Policy debugger" in the navigation. Three inputs are required:

1. The policy (a dropdown; the first entry is selected by default)
2. The action and topic, or the clientId, depending on the action chosen
3. The thing name (optional, but required for policies containing thing variables to resolve)

The result reports the index of the matching statement. When no statement matches, the result is an implicit deny: not a malfunction, but an indication that the policy contains nothing permitting the operation.

The debugger decides on the **policy document** alone and does not ask for a principal type: the policy a given device uses is already stated on the thing and certificate pages.

## Common symptoms and their causes

| Observed symptom | Principal cause |
| --- | --- |
| CONNECT denied while the certificate is sound | The clientId does not equal the thing name (the starter policy pins the resource to `client/<thing>`) |
| Subscription succeeds but no messages arrive | `iot:Subscribe` allowed, `iot:Receive` not allowed |
| SUBACK fails outright | The filter lies outside what `topicfilter/` allows |
| Every operation denied although the policy looks correct | The certificate is not attached to a thing, so statements with thing variables deny in full |
| A statement appears to have no effect | An MQTT `+` is used in the resource, which is not a wildcard under IAM-style matching |
| The policy was changed but device behaviour did not | The policy set is resolved once at CONNECT; existing connections apply the new policy only after reconnecting |

## Retrospective analysis through the event stream

Every authorization denial is recorded as an event carrying the denied action, the topic and the match outcome. The Event stream page in the navigation shows them live, while a thing's timeline aggregates one device's history — which is where a question such as "why did this device drop overnight" can be answered from evidence.

A denial event can be replayed into the debugger in a single action, so the topic need not be transcribed by hand and cannot be mistyped.

## Considerations before changing a policy

Tightening a policy will interrupt devices in service, but not immediately: the policy set is resolved when a connection is established, so already-connected devices are subject to the new rules only after reconnecting. This is why "no anomalies observed after the change" and "devices dropping one after another overnight" are the same event observed at two points in time. Validating against real topics in the debugger beforehand costs substantially less than deploying and rolling back.
