## MIT 6.824: Distributed Systems

### TODO

1. MapReduce ✅ (Mar 23, 2020)
• Write a simple MapReduce program, and then Build a MapReduce library of which the master hands out jobs to workers, and handles failures of workers.
2. Primary Backup Replication Key/Value Service ✅ (Jun 27, 2020)
  • Uses primary/backup replication, assisted by a view service that decides which machines are alive. The view service allows the primary/backup service to work correctly in the presence of network partitions. The view service itself is not replicated, and is a single point of failure.
3. Paxos-based Key/Value Service ✅ (Jul 3, 2020)
  • Uses Paxos protocol to replicate the key/value database with no single point of failure, and handles network partitions correctly. This key/value service is slower than a non-replicated key/value server would be, but is fault tolerant.
4. Sharded Key/Value Service based on Paxos ✅ (Jul 14, 2020)
  • A sharded key/value database where each shard replicates its state using Paxos. This service can perform Put/Get operations in parallel on different shards. It also has a replicated configuration service, which tells the shards what key range they are responsible for. It can change the assignment of keys to shards in response to changing load. This project has the core of a real-world design for thousands of servers.

### Course Description

MIT 6.824 is a core 12-unit graduate subject with lectures, readings, programming labs, an optional project, a mid-term exam, and a final exam. It will present abstractions and implementation techniques for engineering distributed systems. Major topics include fault tolerance, replication, and consistency. Much of the class consists of studying and discussing case studies of distributed systems.

### References
[MIT 6.824 Spring 2015](http://nil.csail.mit.edu/6.824/2015/)
[UCLA CS 134 Spring 2020](http://web.cs.ucla.edu/~ravi/CS134_S20/)