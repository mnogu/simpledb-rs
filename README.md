# simpledb-rs

[SimpleDB](http://cs.bc.edu/~sciore/simpledb/) re-written in Rust.

## Requirements

* `protoc` >= 3

## How to run

### Embedded

```
% cargo run
    Finished dev [unoptimized + debuginfo] target(s) in 0.21s
     Running `target/debug/main`
Connect> foo
creating new database
transaction 1 committed

SQL> create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)
transaction 2 committed
0 records processed

SQL> insert into STUDENT(SId, SName, MajorId, GradYear) values (1, 'joe', 10, 2021)
transaction 3 committed
1 records processed

SQL> select SId, SName, MajorId, GradYear from student
    sid      sname majorid gradyear
-----------------------------------
      1        joe      10     2021
transaction 4 committed
```

### Network

```
% cargo run --bin server
    Finished dev [unoptimized + debuginfo] target(s) in 0.14s
     Running `target/debug/server`
creating new database
transaction 1 committed
```

```
% cargo run --bin client
    Finished dev [unoptimized + debuginfo] target(s) in 0.14s
     Running `target/debug/client`
Connect> //[::1]

SQL> create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)
0 records processed

SQL> insert into STUDENT(SId, SName, MajorId, GradYear) values (1, 'joe', 10, 2021)
1 records processed

SQL> select SId, SName, MajorId, GradYear from student
    sid      sname majorid gradyear
-----------------------------------
      1        joe      10     2021
```

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