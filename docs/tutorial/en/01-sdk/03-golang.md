# Go

AWS SDK for Go v2. For the prerequisites see [Common prerequisites](#/guide/sdk/overview); this page covers only what is specific to Go.

## Dependencies

```bash
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/aws/aws-sdk-go-v2/credentials
go get github.com/aws/aws-sdk-go-v2/service/iot
go get github.com/aws/aws-sdk-go-v2/service/iotdataplane
```

## Pointing at a self-hosted node

Go v2 provides no host parameter; the endpoint is supplied through `BaseEndpoint`. The `/api/aws` path prefix is part of that value, and the SDK appends the operation path with `JoinPath`.

<!-- include: examples/sdk/golang/main.go#client -->

Three available forms, and the trade-offs:

- `o.BaseEndpoint` set per client — recommended. Other clients constructed from the same `cfg` (S3, for instance) are not redirected along with it.
- `config.WithBaseEndpoint(endpoint)` — applies to **all** services; sufficient when this node is the only target.
- The environment variables `AWS_ENDPOINT_URL_IOT` (per service) or `AWS_ENDPOINT_URL` (global) — no code change required.

`config.WithEndpointResolverWithOptions` is marked deprecated by the SDK and should no longer be used.

## Registering a thing and creating a policy

<!-- include: examples/sdk/golang/main.go#thing -->

Determine that a resource already exists by reading the error code through `errors.As`, never by matching the error string:

<!-- include: examples/sdk/golang/main.go#thing-exists -->

## Issuing a certificate and completing the attachments

<!-- include: examples/sdk/golang/main.go#cert -->

Omitting any of the three steps presents as "the device cannot connect", but the causes differ:

- `AttachPolicy` omitted: the certificate is valid but carries no authorization, so CONNECT is rejected.
- `AttachThingPrincipal` omitted: `${iot:Connection.Thing.ThingName}` in the policy cannot be resolved, and an unresolvable variable causes the entire statement to be **denied**, not skipped.

## Publishing a message

<!-- include: examples/sdk/golang/main.go#publish -->

## Reading and writing a device shadow

<!-- include: examples/sdk/golang/main.go#shadow -->

Shadow semantics are covered in a separate chapter: [Device shadow](#/guide/shadow/basics).

## Error codes and corresponding remedies

<!-- include: examples/sdk/golang/main.go#errors -->

## Running the example

```bash
cd examples/sdk/golang
OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=... \
  go run .
```
