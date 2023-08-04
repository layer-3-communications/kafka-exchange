# Kafka Exchange

This uses backpack provide functions for talking to kafka that perform
appropriate encode and decode depending on the message type. Here are
several factors that influence the design of this library:

1. It is nice to be able to avoid relying on a particular implementation
   of network sockets. Backpack is good at dealing with this particular issue.
   Similarly, we are able to avoid committing to a particular serialization
   scheme. In theory, we ought to be able to switch out the binary format
   and sockets in favor of a json format and the filesystem. This could be
   used to test of the more complicated situations.
2. Anything that builds on top of this has application-level errors. Rather
   that making user stack `ExceptT` on top of the opaque type `M` exported
   by this library, this library offers an extension point to its exception
   type with the `Application` data constructor.
3. A kafka client commonly needs to connection to multiple brokers. To consume
   from a single partition, it is necessary to remain connected to two brokers:
   the one that hosts the partition and the one that has the role of
   "group coordinator". The client must remain connected to both of these
   so that offsets can be committed as messages are processed. Producers have
   a slightly different issue. They must connect to all brokers that host any
   of the partitions that they are producing to. We handle this by making
   `Channel.M` accept a type-level `Nat` that communicates how many "logical"
   connections there are. This library invents a nomenclature to distinguish
   connections: logical and physical. Multiple logical connections can be
   backed by the same physical connection. This happens when this library
   detects that it has been asked to establish a connection to a host
   (based on a hostname) that it is already connected to. In the case of
   sharing physical connections, care is taken to avoid the reuse of
   correlation IDs. The user is unaware of this.
