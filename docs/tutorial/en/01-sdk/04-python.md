# Python

boto3. For the prerequisites see [Common prerequisites](#/guide/sdk/overview); this page covers only what is specific to Python.

## Dependencies

```bash
python3 -m venv venv
venv/bin/pip install boto3
```

## Pointing at a self-hosted node

The entry point in boto3 is `endpoint_url`, with the `/api/aws` prefix included in that value. The control plane service is `iot` and the data plane is `iot-data` (botocore's service name; its signing service name is `iotdata`).

<!-- include: examples/sdk/python/main.py#client -->

Supply the CA chain path through `verify` for a self-signed CA. Setting `verify=False` also connects, but it disables certificate validation entirely: an interposed party presenting a different certificate connects just as successfully, and the observable behaviour is indistinguishable from a correct setup.

The corresponding `aws` CLI parameter is `--endpoint-url`:

```bash
aws iot describe-thing --thing-name lamp-01 \
  --endpoint-url http://127.0.0.1:9090/api/aws \
  --region us-east-1
```

## Registering a thing and creating a policy

<!-- include: examples/sdk/python/main.py#thing -->

Determine that a resource already exists by reading `e.response["Error"]["Code"]`, never by matching the exception string: that string carries a request id.

## Issuing a certificate and completing the attachments

<!-- include: examples/sdk/python/main.py#cert -->

Omitting any of the three steps presents as "the device cannot connect", but the causes differ:

- `attach_policy` omitted: the certificate is valid but carries no authorization, so CONNECT is rejected.
- `attach_thing_principal` omitted: `${iot:Connection.Thing.ThingName}` in the policy cannot be resolved, and an unresolvable variable causes the entire statement to be **denied**, not skipped.

## Publishing a message

<!-- include: examples/sdk/python/main.py#publish -->

## Reading and writing a device shadow

<!-- include: examples/sdk/python/main.py#shadow -->

Note that the `payload` returned by `get_thing_shadow` is a **stream** and can be consumed by `.read()` only once; it is empty thereafter. Retain the content locally if it must be emitted more than once.

Shadow semantics are covered in a separate chapter: [Device shadow](#/guide/shadow/basics).

## Error codes and corresponding remedies

<!-- include: examples/sdk/python/main.py#errors -->

## Running the example

```bash
cd examples/sdk/python
OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=... \
  venv/bin/python main.py
```
