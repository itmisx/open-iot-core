# Policy documents, actions and resources

A policy consists of a number of statements. Each statement declares three things: allow or deny, the action it applies to, and the resource it acts upon.

## The starter policy

The policy below is the one attached by default when a certificate is issued. `${iot:Connection.Thing.ThingName}` must be left verbatim: it is resolved against the current connection at decision time.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "iot:Connect",
      "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"
    },
    {
      "Effect": "Allow",
      "Action": ["iot:Publish", "iot:Receive"],
      "Resource": "arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/*"
    },
    {
      "Effect": "Allow",
      "Action": "iot:Subscribe",
      "Resource": "arn:aws:iot:*:*:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/*"
    }
  ]
}
```

It allows nothing beyond the device's own subtree under `$aws/things/<thing>/`: shadows, jobs, and whatever business topics are placed under that prefix. To enable a different prefix, add a statement of the same shape.

## The five actions and their resource types

| Action | Resource type | Evaluated at |
| --- | --- | --- |
| `iot:Connect` | `client/` | The CONNECT packet; the resource is the clientId |
| `iot:Publish` | `topic/` | Every PUBLISH, against the concrete topic |
| `iot:Subscribe` | `topicfilter/` | SUBSCRIBE; the resource is the filter itself, wildcards permitted |
| `iot:Receive` | `topic/` | Every delivery, one concrete topic at a time |
| `iot:RetainPublish` | `topic/` | Publishing a retained message, evaluated separately from `iot:Publish` |

The resource type cannot be wildcarded: `arn:aws:iot:*:*:*/…` allows nothing whatsoever. One of `client/`, `topic/` or `topicfilter/` must be stated explicitly.

## Subscribe and Receive are evaluated separately

This is the most common cause of "the connection succeeds but no data arrives":

- `iot:Subscribe` evaluates the **filter** that was submitted, under `topicfilter/`.
- `iot:Receive` evaluates **the concrete topic** actually delivered, under `topic/`.

When only Subscribe is allowed, SUBACK succeeds while no message can be delivered, and the device observes a successful subscription. In the opposite case, where only Receive is allowed, SUBACK fails outright, which is considerably easier to diagnose.

## The two kinds of wildcard have different semantics

| Where used | Symbols | Crosses slashes |
| --- | --- | --- |
| Policy resources (IAM style) | `*`, `?` | Yes. `topic/a/*` matches `a/b/c` |
| MQTT filters | `+`, `#` | `+` matches one level, `#` matches all remaining levels |

Placing MQTT's `+` in a policy resource raises no error but never matches, and the observable symptom is that the statement appears to have no effect.

## Deny takes precedence; no match means denial

An explicit `Deny` overrides any `Allow`. When no statement matches, the result is an **implicit deny**: a policy is an allow list, and what is not declared is not permitted.

When tightening permissions, do not adopt the approach of "adding a Deny to switch it off"; identify which `Allow` opened the operation instead. Otherwise the combined outcome of several stacked policies cannot be determined with confidence.

## The clientId normally has to equal the thing name

In the starter policy the resource for `iot:Connect` is `client/${iot:Connection.Thing.ThingName}`, so the clientId must equal the thing name. Graphical MQTT clients default to a random clientId and are rejected; because the rejection occurs during CONNECT, it presents much like a certificate problem.
