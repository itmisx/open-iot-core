# Java

AWS SDK for Java v2. For the prerequisites see [Common prerequisites](#/guide/sdk/overview); this page covers only what is specific to Java.

## Dependencies

The control plane and the data plane are two artifacts and both must be declared. Versions are managed centrally by the BOM.

<!-- include: examples/sdk/java/pom.xml#deps -->

## Pointing at a self-hosted node

The entry point in Java is `endpointOverride`, which accepts a `URI`. The `/api/aws` prefix belongs in that URI's path, and the SDK appends the operation path after it.

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#client -->

The two clients must be constructed separately rather than switched by method on a single client: the signing service name is determined by the client type (`iot` and `iotdata`) and is not a parameter.

## Registering a thing and creating a policy

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#thing -->

The lambda form of `createThing` (`b -> b.thingName(thing)`) is the idiomatic v2 style and removes one line of `XxxRequest.builder()...build()`.

## Issuing a certificate and completing the attachments

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#cert -->

Omitting any of the three steps presents as "the device cannot connect", but the causes differ:

- `attachPolicy` omitted: the certificate is valid but carries no authorization, so CONNECT is rejected.
- `attachThingPrincipal` omitted: `${iot:Connection.Thing.ThingName}` in the policy cannot be resolved, and an unresolvable variable causes the entire statement to be **denied**, not skipped.

## Publishing a message

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#publish -->

## Reading and writing a device shadow

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#shadow -->

Shadow semantics are covered in a separate chapter: [Device shadow](#/guide/shadow/basics).

## Error codes and corresponding remedies

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#errors -->

## Running the example

```bash
cd examples/sdk/java
OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=... \
  mvn -q compile exec:java
```
