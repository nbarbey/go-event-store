# An event store library in go on top of PostgreSQL

This library implements an event store on top of PostgreSQL.
It handles typing and versioning of events.
It handles marshaling and unmarshaling so that event payloads
are stored uniformely as byte arrays in PostgreSQL.