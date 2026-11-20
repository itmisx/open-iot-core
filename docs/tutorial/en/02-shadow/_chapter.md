# Device shadow

A device shadow is a readable and writable JSON document describing a device's state: the cloud writes `desired`, the device reports `reported`, and the node computes the difference between the two (`delta`) and pushes it to the device. A desired state written while the device is offline is delivered once the device reconnects.
