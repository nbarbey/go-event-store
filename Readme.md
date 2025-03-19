# An event store library in go on top of PostgreSQL

This library implements an event store on top of PostgreSQL.
It handles typing and versioning of events.
It handles marshalling and unmarshalling so that event payloads
are stored uniformly as byte arrays in PostgreSQL.

This is heavily inspired by Marten https://martendb.io/.
This is also inspired by a talk called "Let's build event store in one hour!" by Oskar Dudycz
which he gave at NDC Oslo in 2022. (see https://www.youtube.com/watch?v=gaoZdtQSOTo).

It has been used as a pedagogic tool to explain the event sourcing architecture
using the banking kata (https://kata-log.rocks/banking-kata) with good success.

This code is not production ready as it has not been tested in production yet.

## example

// https://github.com/nbarbey/go-event-store/blob/1f3260a2cbe64b54e77c252aba8b02e84c0e2d9d/eventstore/eventstore_example_test.go?plain=1
