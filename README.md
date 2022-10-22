# simpledb-rs

[SimpleDB](http://cs.bc.edu/~sciore/simpledb/) re-written in Rust.

## Requirements

* `protoc` >= 3

## Status

- [x] Disk and File Management
- [x] Memory Management
- [x] Transaction Management
- [x] Record Management
- [x] Metadata Management
- [x] Query Processing
- [x] Parsing
- [x] Planning
- [ ] JDBC Interfaces
  - [x] Uses [gRPC](https://grpc.io/) and [Protocol Buffers](https://developers.google.com/protocol-buffers) instead
- [x] Indexing
- [x] Materialization and Sorting
- [x] Effective Buffer Utilization
- [x] Query Optimization

## References

* [Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7)
* [The SimpleDB Database System](http://cs.bc.edu/~sciore/simpledb/)