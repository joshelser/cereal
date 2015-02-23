Accumulo Object Serialization
=============================

[![Build Status](https://travis-ci.org/joshelser/cereal.svg?branch=master)](https://travis-ci.org/joshelser/cereal)

Apache Accumulo is a sorted, distributed key/value store. As a developer, you
don't deal with keys and value -- you deal with objects. At the very base, each
record has a collection of attributes which, together, comprise some state. Mapping
these objects into a key/value structure is not difficult, but it can be very pedantic.

Use of serialization libraries such a Apache Thrift and Google's Protocol Buffers
provide the means to automatically generate Java code for simple objects. These libraries
also have the added benefit of naturally handling drifting schemas -- an object that has
a field when it was created might not have that field six months later (if correctly defined).

The attributes on a message (a name to refer to these generated classes) can be easily mapped
into Accumulo columns in an Accumulo row. This can be abstracted away from developers by allowing
them to define the Accumulo row for a message, and, for each attribute in the record, define an
optional grouping (the column family), the name of the field (the column qualifier), and
the visibility of the field (the column visibility, obviously).

This mapping, along with the introspection provided by the generated classes, provides a minimal
implementation that a developer must write to get basic serialization/deserialization with Accumulo.

Cereal Schema
-------------

Cereal represents your simple objects in Accumulo, one per row. It attempts to follow the approach
outlined in the BigTable paper: The row uniquely identifies the message, and the columns within that
row represent attributes or fields of that message. For each column, the family serves to group similarly
accessed fields together, the qualifier stores the name of the field, and the visibility restricts access
to that field. The value is obvious.

Making the cereal
-----------------

Cereal expects the developer to provide one implementation for each object/message to be serialized:
a Mapping. A Mapping controls the serialization details for a message: the fields in the message,
the Accumulo row ID, and how to rebuild the message from its serialized form (Key/Value pairs). A field
in a message defines an optional grouping (the column family), an optional visibility (the column visibility)
in addition to the required name and value for that field.

For plain old Java objects (POJOs), the developer must implement both the methods that get the
fields for a message and the deserialization back into the message. For Thrift and ProtocolBuffer
messages, this is handled automatically through the abstract ThriftStructMapping and ProtocolBufferMapping.

Eating the cereal
-----------------

With a Mapping defined for a message class, instances of that message can be provided to the Store
to be serialized and instances can be retrieved from Accumulo in the message type.

```
  Connector conn = getConnector();
  Registry registry = new RegistryImpl();
  registry.add(new PersonMapping());

  try (Store store = new StoreImpl(registry, conn, tableName)) {
    store.write(Collections.singleton(p));
    store.flush();

    Person pCopy = store.read("Bob_Joe_Franklin", Person.class);
    System.out.println("Copy: " + pCopy);
  }
```

The developer can deal wholly in terms of the messages, not having be deal with Keys or Values. Hooray.

Examples
--------

End to end examples exist for POJO, Thrift message and ProtocolBuffer messages, all representations
of the same data: a person's name, age, height and weight.

 * [POJO Example][1]
 * [Thrift Example][2]
 * [Protobuf Example][3]

Work in Progress
----------------

This project should still be considered a work in progress, but I plan to see it through as
I believe there is much worth in what it can provide.

- [x] Basic Thrift support
- [x] Basic Protobuf support
- [ ] Unit test coverage for core
- [ ] Pluggable serialization for primitives
- [ ] Protobuf abstract control for grouping and visibility
- [ ] Thrift abstract control for grouping and visibility
- [ ] Protobuf complex types
  - [ ] Repeated fields
  - [ ] Nested messages
- [ ] Thrift complex types
  - [ ] List fields
  - [ ] Map fields
  - [ ] Set fields
  - [ ] Nested structs
  - [ ] Enums
- [ ] End-to-end tests with MiniAccumuloCluster
- [ ] Performance benchmarks

[1]: http://github.com/joshelser/cereal/tree/master/examples/src/main/java/cereal/examples/pojo/PojoExample.java
[2]: http://github.com/joshelser/cereal/tree/master/examples/src/main/java/cereal/examples/thrift/ThriftExample.java
[3]: http://github.com/joshelser/cereal/tree/master/examples/src/main/java/cereal/examples/protobuf/ProtobufExample.java
