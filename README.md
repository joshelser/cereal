Accumulo Object Serialization
=============================

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

This mapping, along with the introspection provided by the generated classes provides a minimal
implementation that a developer must write to get basic serialization/deserialization with Accumulo.
