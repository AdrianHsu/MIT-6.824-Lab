你好！
很冒昧用这样的方式来和你沟通，如有打扰请忽略我的提交哈。我是光年实验室（gnlab.com）的HR，在招Golang开发工程师，我们是一个技术型团队，技术氛围非常好。全职和兼职都可以，不过最好是全职，工作地点杭州。
我们公司是做流量增长的，Golang负责开发SAAS平台的应用，我们做的很多应用是全新的，工作非常有挑战也很有意思，是国内很多大厂的顾问。
如果有兴趣的话加我微信：13515810775  ，也可以访问 https://gnlab.com/，联系客服转发给HR。
## MIT 6.824: Distributed Systems

> 🏃 MIT 6.824 is where my journey of Distributed Systems began. All projects are my own individual works.

### TODO

1. MapReduce 
  * ✅ (Mar 23, 2020)
  * Write a simple MapReduce program, and then Build a MapReduce library of which the master hands out jobs to workers, and handles failures of workers.
2. Primary Backup Replication Key/Value Service 
  * ✅ (Jun 27, 2020)
  * Uses primary/backup replication, assisted by a view service that decides which machines are alive. The view service allows the primary/backup service to work correctly in the presence of network partitions. The view service itself is not replicated, and is a single point of failure.
3. Paxos-based Key/Value Service 
  * ✅ (Jul 3, 2020)
  * Uses Paxos protocol to replicate the key/value database with no single point of failure, and handles network partitions correctly. This key/value service is slower than a non-replicated key/value server would be, but is fault tolerant.
4. Sharded Key/Value Service based on Paxos 
  * ✅ (Jul 14, 2020)
  * A sharded key/value database where each shard replicates its state using Paxos. This service can perform Put/Get operations in parallel on different shards. It also has a replicated configuration service, which tells the shards what key range they are responsible for. It can change the assignment of keys to shards in response to changing load. This project has the core of a real-world design for thousands of servers.

### Course Description

MIT 6.824 is a core 12-unit graduate subject with lectures, readings, programming labs, an optional project, a mid-term exam, and a final exam. It will present abstractions and implementation techniques for engineering distributed systems. Major topics include fault tolerance, replication, and consistency. Much of the class consists of studying and discussing case studies of distributed systems.

### References
* [MIT 6.824 Spring 2015](http://nil.csail.mit.edu/6.824/2015/)
* [UCLA CS 134 Spring 2020](http://web.cs.ucla.edu/~ravi/CS134_S20/)
