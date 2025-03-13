# An event store library in go on top of PostgreSQL

This library implements an event store on top of PostgreSQL.
It handles typing and versioning of events.
It handles marshaling and unmarshalling so that event payloads
are stored uniformly as byte arrays in PostgreSQL.