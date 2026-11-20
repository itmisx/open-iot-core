# Common prerequisites

The three languages differ by a few lines of code, but the six conventions below are identical for all of them, and they account for most integration failures in practice. They are stated once here and not repeated on the language-specific pages.

## The endpoint must include the `/api/aws` prefix

The compatibility surface is mounted under `/api/aws` on the **admin port**, not on a separate port. The endpoint therefore takes the form `http://127.0.0.1:9090/api/aws`, with the prefix as an integral part of it.

| Surface | Responsibility | Signing service name |
| --- | --- | --- |
| Control plane | Register and delete things, issue and revoke certificates, maintain policies | `iot` |
| Data plane | Publish messages, read and write shadows | `iotdata` |
| Device WebSocket | A device connecting to `/mqtt` under its own SigV4 signature | `iotdevicegateway` |

The control plane and the data plane share the `/api/aws` prefix but use **different signing service names**, which is why they are two distinct clients in every SDK. A data plane call signed as `iot` is rejected exactly as a malformed signature would be, and the response does not indicate that the service name is wrong.

A missing prefix produces a symptom that does not point at the cause: the request reaches the console index page and returns HTML, so the SDK reports that the response is not JSON — the Go SDK's literal message is `invalid character '<'`. An angle bracket indicates the prefix is absent.

## Issuing an access key

Access keys are not held in a configuration file; the node issues them, and they are stored in replicated metadata, so every node in a cluster honours them. There are two routes:

- The lower card on the console's Access page, visible to `admin` only. The secret is displayed exactly once, in the dialog that confirms creation.
- The admin API, called with the bootstrap token:

```bash
curl -s -X POST http://127.0.0.1:9090/api/admin/v1/awskeys \
  -H "Authorization: Bearer $(cat data/admin-token)" \
  -H 'Content-Type: application/json' \
  -d '{"role":"operator","note":"ci","expires":"2027-01-01T00:00:00Z"}'
```

The `id` and `secret` in the response correspond to `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. The secret cannot be retrieved a second time: it is stored encrypted and is decrypted only to verify a signature.

> Console users and access keys are two distinct classes of principal with no overlap. A console user holds an admin token, scoped to the console and `/api/admin/v1`; an access key is used by SDK clients and scoped to the set of actions callable on `/api/aws`. The role names are identical in meaning on both sides, but the credentials are not interchangeable: an admin token presented to `/api/aws` yields 401, and an access key presented to the admin API yields 401 as well.

## Roles determine the callable action set

| Role | Scope |
| --- | --- |
| `reader` | Read only: list things, read policies, read shadows, search |
| `operator` | Routine writes: register and delete things, issue and revoke certificates, maintain policies, publish, write shadows |
| `admin` | Additionally the trust root: register and delete a custom CA, issue credentials |

The required level is declared at each route's mount point and is **not inferred from the HTTP method**: `SearchIndex` is a POST whose semantics are a read, and `GET /role-aliases/{n}/credentials` is a GET whose semantics are credential issuance.

## 401 and 403 denote different failures

| Status | Error code | Remedy |
| --- | --- | --- |
| 401 | `UnauthorizedException` | Incorrect, disabled or expired key, or excessive local clock skew (SigV4 is timestamped) |
| 403 | `AccessDeniedException` | The signature is valid but the role is insufficient; use a credential with a higher role |

A nonexistent key and an invalid signature are deliberately collapsed into the same 401; otherwise the endpoint would become a means of probing whether a given id exists. Disabled and expired keys are folded into the same response for the same reason.

## The region must be on the allow list

The node's `aws_api.region` setting is an **allow list**, not a default. A region absent from the list causes the request to be rejected rather than rewritten to the configured one. Declaring the region alongside the endpoint in CI eliminates an entire class of "401 after changing machines".

## A self-signed CA must be trusted explicitly

Under HTTPS the node's server certificate is signed by its own CA. The chain resides at `pki/ca-chain.pem` in the data directory, and all three SDKs provide a means of supplying it; see the respective language page.

## Subsequent pages

- [Java](#/guide/sdk/java)
- [Go](#/guide/sdk/golang)
- [Python](#/guide/sdk/python)

All code on those pages is taken from genuinely compilable example projects in the repository (`examples/sdk/`) rather than from a transcribed copy.
