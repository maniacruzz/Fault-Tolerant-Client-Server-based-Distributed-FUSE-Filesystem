# Fault-Tolerant-Client-Server-based-Distributed-FUSE-Filesystem

Implementation of client-server based distributed FUSE filesystem with fault tolerance handling server failures and data corruption.

Developed Client/Server model for FUSE file system using XML-RPC protocol in Python. The data blocks were distributed across multiple servers in a round-robin fashion. Used hashing to achieve load balancing, implemented fault tolerant server using redundancy that handles server crash and corruptions in file system, executed all or nothing atomicity for writes by versioning.
