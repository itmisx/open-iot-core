# Policy variables

Policy variables allow a single policy to be distributed to every device while each device can access only its own resources. Variables are resolved against the **current connection** at decision time.

## Available variables

| Variable | Resolves to |
| --- | --- |
| `${iot:ClientId}` | The clientId from this connection's CONNECT packet |
| `${iot:Connection.Thing.ThingName}` | The thing the certificate is attached to |
| `${iot:Connection.Thing.IsAttached}` | Whether the certificate is attached to a thing |
| `${iot:Connection.Thing.Attributes[name]}` | A given attribute of the thing |
| `${iot:Certificate.SerialNumber}` | The client certificate's serial number |
| `${iot:Certificate.Subject.CommonName}` | The CN in the certificate subject (Issuer takes the same form) |

One policy can cover tens of thousands of devices: `topic/$aws/things/${iot:Connection.Thing.ThingName}/*` resolves to a different value for each of them, and none can reach another's topics.

## An unresolvable variable denies the entire statement

This rule is deliberate and is the most security-critical item on this page: a variable that cannot be resolved does **not** degrade to an empty string.

An empty string would turn `topic/${iot:Connection.Thing.ThingName}/x` into `topic//x`, and combined with a slash-crossing `*` in the resource, a statement intended to allow only the device's own topics could allow every device's topics.

Consequently, when a certificate is not attached to a thing (`AttachThingPrincipal` was not performed), statements containing thing variables allow **nothing at all**. The symptom is "the certificate is correct and the policy is attached, yet every operation is denied"; in that case verify the certificate-to-thing attachment first.

## Refining authorization by attribute

Thing attributes may be used in policies to partition permissions by production line, site or model:

```json
{
  "Effect": "Allow",
  "Action": "iot:Publish",
  "Resource": "arn:aws:iot:*:*:topic/sites/${iot:Connection.Thing.Attributes[site]}/*"
}
```

Once an attribute changes, subsequent decisions use the new value immediately. This also means that editing an attribute constitutes a **permission change**: it does not look like one, but the effect is identical.

## Use variables rather than enumerating thing names

Listing tens of thousands of thing names in a single `Resource` works, but the document will exceed the size limit, and every additional device requires editing the policy and redistributing it. A variable expresses the same intent at constant size.
