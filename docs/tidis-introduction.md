# Tidis

## What is Tidis
[Tidis](https://github.com/tidb-incubator/tidis) is open source now!

`Tidis` is the service layer for TiKV, aims to provide redis protocol compatiable distributed storage service powered by PingCAP. Redis protocol has been implemented with multiple datatypes (string/hash/list/set/sortedset) support.

`Tidis` has been completely refactored and rewritten in `Rust` for better performance and lower latency, and added more features support, such as Lua scripts, TLS connections, meta key split and more.

## Background

Redis is an in-memory data structure store, used by lots of companies and projects. It is a very good solution for caching and storing data. But it has some limitations:

- High cost of memory, not suitable for store huge amounts of data
- Not easy to manage, and not easy to scale out with huge datasets
- Lots of use cases are relying on protocol and api, not the extreme performance
- No global transaction support
- No data persistence guarantee

People believe that it is a good idea to build a redis compatiable distributed storage service based on lower cost storage medium. As a result, we can see many products build with this idea, such as `ssdb`, `pika`, `kvrocks` and so on. But they are all compute storage aggregated architechture, which means it's very complex and complicated to build the basic ability of storage replication, high availability, data sharding, data scaling and so on. Not only it is difficult to implement, but also complicated to operate in production.

Considering the complexities, we take a different approach and build a lite computing service layer on top of [TiKV](https://tikv.org/), a distributed storage system based on `Raft` consensus algorithm. `TiKV` is an excellent building block for storage systems, it has a lot of features such as high availability, data sharding, data scaling, global transaction and so on. We just work on the data model and computation which is also known as the service layer. 

Of cause we are not the only one who has similar ideas. Both `Titan` and `Tidis 1.0` were designed with similar architecture, and they were all popular back to early days and grew their community to some extent. Unfortunately, due to lack of maintenance, their communities are inactive nowadays and leaving lots of features not implemented. We believe the community is always looking forward to seeing alternative projects that are mature and feature complete. Therefore we decide to take over what Tidis 1.0 left behind, redesign and implement the next generation of Tidis with some important yet missing features added, such as Lua script, TLS/SSL, lock optimization, one phase commit, asynchronous commit and many other improvements.

## Features

* Multiple protocol support
* Linear scale-out
* Storage and compute disaggregation
* Highly durable and available with strong consistency
* Global transaction support

## Architecture

*Architechture of Tidis*

![architecture](https://raw.githubusercontent.com/tidb-incubator/tidis/master/docs/tidis-arch.png)

*Architechture of TiKV*

![](https://tikv.org/img/basic-architecture.png)
- Placement Driver (PD): PD is the brain of the TiKV system which manages the metadata about Nodes, Stores, Regions mapping, and makes decisions for data placement and load balancing. PD periodically checks replication constraints to balance load and data automatically.
- Node: A physical node in the cluster. Within each node, there are one or more Stores. Within each Store, there are many Regions.
- Store: There is a RocksDB within each Store and it stores data in local disks.
- Region: Region is the basic unit of Key-Value data movement and corresponds to a data range in a Store. Each Region is replicated to multiple Nodes. These multiple replicas form a Raft group. A replica of a Region is called a Peer.

## Running

- Run clients

For redis protocol, you can use the official redis clients, such as `redis-cli`.

```
redis-cli -p 6379
tidis> SET mykey "Hello"
"OK"
tidis> GET mykey
"Hello"
tidis> EXPIRE mykey 10
(integer) 1
# 10 seconds later
tidis> GET mykey
(nil)
tidis> RPUSH mylist "one"
(integer) 1
tidis> RPUSH mylist "two"
(integer) 2
tidis> RPUSH mylist "three"
(integer) 3
tidis> LRANGE mylist 0 0
1) "one"
tidis> LRANGE mylist -3 2
1) "one"
2) "two"
3) "three"
tidis> LRANGE mylist -100 100
1) "one"
2) "two"
3) "three"
tidis> LRANGE mylist 5 10
(nil)
tidis> ZADD myzset 1 "one"
(integer) 1
tidis> ZADD myzset 2 "two"
(integer) 1
tidis> ZADD myzset 3 "three"
(integer) 1
tidis> ZREMRANGEBYSCORE myzset 0 1
(integer) 1
tidis> ZRANGE myzset 0 5 WITHSCORES
1) "two"
2) "2"
3) "three"
4) "3"
```

## Supported commands

### Keys

    +-----------+-------------------------------------+
    |  pexpire  | pexpire key int                     |
    +-----------+-------------------------------------+
    | pexpireat | pexpireat key timestamp(ms)         |
    +-----------+-------------------------------------+
    |   expire  | expire key int                      |
    +-----------+-------------------------------------+
    |  expireat | expireat key timestamp(s)           |
    +-----------+-------------------------------------+
    |    pttl   | pttl key                            |
    +-----------+-------------------------------------+
    |    ttl    | ttl key                             |
    +-----------+-------------------------------------+
    |    type   | type key                            |
    +-----------+-------------------------------------+
    |    scan   | scan "" [count 10] [match "^pre*"]  |
    +-----------+-------------------------------------+
    |    ping   | ping                                |
    +-----------+-------------------------------------+

### String

    +-----------+-------------------------------------+
    |  command  |               format                |
    +-----------+-------------------------------------+
    |    get    | get key                             |
    +-----------+-------------------------------------+
    |    set    | set key value [EX sec|PX ms][NX|XX] | 
    +-----------+-------------------------------------+
    |    del    | del key1 key2 ...                   |
    +-----------+-------------------------------------+
    |    mget   | mget key1 key2 ...                  |
    +-----------+-------------------------------------+
    |    mset   | mset key1 value1 key2 value2 ...    |
    +-----------+-------------------------------------+
    |    incr   | incr key                            |
    +-----------+-------------------------------------+
    |   incrby  | incr key step                       |
    +-----------+-------------------------------------+
    |    decr   | decr key                            |
    +-----------+-------------------------------------+
    |   decrby  | decrby key step                     |
    +-----------+-------------------------------------+
    |   strlen  | strlen key                          |
    +-----------+-------------------------------------+

### Hash

    +------------+------------------------------------------+
    |  Commands  | Format                                   |
    +------------+------------------------------------------+
    |    hget    | hget key field                           |
    +------------+------------------------------------------+
    |   hstrlen  | hstrlen key field                        |
    +------------+------------------------------------------+
    |   hexists  | hexists key field                        |
    +------------+------------------------------------------+
    |    hlen    | hlen key                                 |
    +------------+------------------------------------------+
    |    hmget   | hmget key field1 field2 field3...        |
    +------------+------------------------------------------+
    |    hdel    | hdel key field1 field2 field3...         |
    +------------+------------------------------------------+
    |    hset    | hset key field value                     |
    +------------+------------------------------------------+
    |   hsetnx   | hsetnx key field value                   |
    +------------+------------------------------------------+
    |    hmset   | hmset key field1 value1 field2 value2... |
    +------------+------------------------------------------+
    |    hkeys   | hkeys key                                |
    +------------+------------------------------------------+
    |    hvals   | hvals key                                |
    +------------+------------------------------------------+
    |   hgetall  | hgetall key                              |
    +------------+------------------------------------------+
    |   hincrby  | hincrby key step                         |
    +------------+------------------------------------------+

### List

    +------------+---------------------------------------------+
    |  commands  |         format                              |
    +------------+---------------------------------------------+
    |    lpop    | lpop key                                    |
    +------------+---------------------------------------------+
    |    rpush   | rpush key  item                             |
    +------------+---------------------------------------------+
    |    lpush   | lpush key  item                             |
    +------------+---------------------------------------------+
    |    rpop    | rpop key                                    |
    +------------+---------------------------------------------+
    |    llen    | llen key                                    |
    +------------+---------------------------------------------+
    |   lindex   | lindex key index                            |
    +------------+---------------------------------------------+
    |   lrange   | lrange key start stop                       |
    +------------+---------------------------------------------+
    |    lset    | lset key index value                        |
    +------------+---------------------------------------------+
    |    ltrim   | ltrim key start stop                        |
    +------------+---------------------------------------------+
    |   linsert  | linsert key <BEFORE | AFTER> pivot element  |
    +------------+---------------------------------------------+

### Set

    +-------------+-------------------------------------+
    |   commands  |             format                  |
    +-------------+-------------------------------------+
    |     sadd    | sadd key member1 [member2 ...]      |
    +-------------+-------------------------------------+
    |    scard    | scard key                           |
    +-------------+-------------------------------------+
    |  sismember  | sismember key member                |
    +-------------+-------------------------------------+
    |  smismember | smismember key member [member2 ...] |
    +-------------+-------------------------------------+
    |   smembers  | smembers key                        |
    +-------------+-------------------------------------+
    |     srem    | srem key member                     |
    +-------------+-------------------------------------+
    |     spop    | spop key [count]                    |
    +-------------+-------------------------------------+
    | srandmember | spop key [count]                    |
    +-------------+-------------------------------------+

### Sorted set

    +------------------+---------------------------------------------------------------+
    |     commands     |                             format                            |
    +------------------+---------------------------------------------------------------+
    |       zadd       | zadd key member1 score1 [member2 score2 ...]                  |
    +------------------+---------------------------------------------------------------+
    |       zcard      | zcard key                                                     |
    +------------------+---------------------------------------------------------------+
    |      zrange      | zrange key start stop [WITHSCORES]                            |
    +------------------+---------------------------------------------------------------+
    |     zrevrange    | zrevrange key start stop [WITHSCORES]                         |
    +------------------+---------------------------------------------------------------+
    |   zrangebyscore  | zrangebyscore key min max [WITHSCORES][LIMIT offset count]    |
    +------------------+---------------------------------------------------------------+
    | zrevrangebyscore | zrevrangebyscore key max min [WITHSCORES][LIMIT offset count] |
    +------------------+---------------------------------------------------------------+
    | zremrangebyscore | zremrangebyscore key min max                                  |
    +------------------+---------------------------------------------------------------+
    | zremrangebyrank  | zremrangebyscore key start stop                               |
    +------------------+---------------------------------------------------------------+
    |      zcount      | zcount key                                                    |
    +------------------+---------------------------------------------------------------+
    |      zscore      | zscore key member                                             |
    +------------------+---------------------------------------------------------------+
    |      zrank       | zrank key member                                              |
    +------------------+---------------------------------------------------------------+
    |       zrem       | zrem key member1 [member2 ...]                                |
    +------------------+---------------------------------------------------------------+
    |      zpopmin     | zpopmin key [count]                                           |
    +------------------+---------------------------------------------------------------+
    |      zpopmax     | zpopmax key [count]                                           |
    +------------------+---------------------------------------------------------------+
    |      zincrby     | zincrby key increment member                                  |
    +------------------+---------------------------------------------------------------+

### Lua

    +-------------+-----------------------------------------------------+
    |   commands  |             format                                  |
    +-------------+-----------------------------------------------------+
    |     eval    | eval script numkeys [key [key ...]] [arg [arg ...]] |
    +-------------+-----------------------------------------------------+
    |    evalsha  | evalsha sha1 numkeys [key [key ...]] [arg [arg ...]]|
    +-------------+-----------------------------------------------------+
    | script load | script load script                                  |
    +-------------+-----------------------------------------------------+
    | script flush| script flush                                        |
    +-------------+-----------------------------------------------------+
    |script exists| script exists sha1 [sha1 ...]                       |
    +-------------+-----------------------------------------------------+

### Security

    +-------------+----------------------+
    |   commands  |      format          |
    +-------------+----------------------+
    |    auth     | auth password        |
    +-------------+----------------------+


### Debug

    +-------------+----------------------+
    |   commands  |      format          |
    +-------------+----------------------+
    |    debug    | debug profiler_start |
    +-------------+----------------------+
    |    debug    | debug profiler_stop  |
    +-------------+----------------------+

### Cluster

    +-----------------+------------+
    |   command       |    support |
    +-----------------+------------+
    |  cluster nodes  |    Yes     |
    +-----------------+------------+
    |  cluster info   |    Yes     |
    +-----------------+------------+


### Transaction

    +---------+---------+
    | command | support |
    +---------+---------+
    |  multi  | Yes     |
    +---------+---------+
    |   exec  | Yes     |
    +---------+---------+
    | discard | Yes     |
    +---------+---------+

### Client Management

    +-----------------+------------+
    |   command       |    support |
    +-----------------+------------+
    |  client setname |    Yes     |
    +-----------------+------------+
    |  client getname |    Yes     |
    +-----------------+------------+
    |  client id      |    Yes     |
    +-----------------+------------+
    |  client list    |    Yes     |
    +-----------------+------------+
    |  client kill    |    Yes     |
    +-----------------+------------+


## TLS/SSL support

TLS/SSL encryption is necessary for security, especially in public access envrionment, such as providing cloud services in AWS, GCP or Azure cloud.

`Tidis` support `one-way` and `two-way` TLS authentication handshake. In `one-way` authentication, you can use the ca certificate and password for security transport and authticate. If you choose `two-way` authentication, it is safe using ca certificate with client-side key and certificate without password.

## Global transaction

Thanks to the global transaction mechanism in `TiKV` cluster, `Tidis` can support the global transaction easily. Use `MULTI/EXEC/DISCARD` command just like `Redis Cluster` but without caring about the `CROSSSLOT` error, just use it like a single `Redis` instance.

In `Tidis`, there are two kinds of transaction models, `optimistic` and `perssimistic` models.

Perssimistic transaction is prefered when you have a lot hot-key writes in parallel. Otherwise, you should use optimistic transaction instead for better performance.

You can refer to the documents in TiDB [optimistic transaction](https://docs.pingcap.com/tidb/dev/optimistic-transaction) and [perssimistic transaction](https://docs.pingcap.com/tidb/dev/perssimistic-transaction).

In addition, the `1pc` and `async commit` options are helpful for better performance in most use cases. You can refer to the documents in PingCAP blog [AsyncCommit, the Accelerator for Transaction Commit in TiDB 5.0](https://www.pingcap.com/blog/async-commit-the-accelerator-for-transaction-commit-in-tidb-5-0/) for details.

## Lua Script

`Tidis` use `mlua` library to interpret lua scripts. We can use `EVAL/EVALSHA` to execute lua script with global transaction support, without caring about the `CROSSSLOT` error either.

The lua script will be running in a new transaction context, so all read and writes in the lua script are atomic guranteed.

All lua script e2e test cases are located in [test/test_lua.py](https://github.com/tidb-incubator/tidis/blob/master/test/test_lua.py).

## Asynchronous key deletion

For collection keys with thousands of items, deletion will be a time-costing operation, enable the async deletion configuration could greatly reduce the operation time.

The big key deletion task will be run in background and the request will be response to user immediately without waiting for the background task's completion. We have to maintain multiple versions of the same key if the old version has been deleted in asynchronous way. If there are some version are in the deletion queue, next new version of the key will be monotonic increase from the latest version of the key. Otherwise, the new version will be start fresh from 0.

For big keys with thousands of elements deletion, the time cost decrease from seconds to milliseconds.

|      type      |    hash    |    list    |    set     | sorted set |
| :------------: | :--------: | :--------: | :--------: | :--------: |
| sync deletion  | 1.911778 s | 2.047429 s | 2.145035 s | 4.892823 s |
| async deletion | 0.005159 s | 0.004694 s | 0.005370 s | 0.005403 s |

## Super batch feature support

Use super batch enabled could have obvious performance optimization, but you can tunning it by your business pressure.

Super batch is a feature that can reduce the number of user requests round-trips to TiKV. It is enabled by default, and can be disabled by setting to `allow_batch=false` in the configuration file. And more configuration options are available for tuning. `max_batch_wait_time` is the maximum waiting time for the batch, and `max_batch_size` is the maximum number of keys in one batch. `overload_threshold` is the threshold of the backend TiKV server's load, if the load is higher than this value, the batch request will be sent immediately.

You can tunning the parameters according to your business pressure. In the following test, we set `max_batch_wait_time` to 10ms, `overload_threshold` to 0, and `max_batch_size` varies. The test result shows that the throughput increase 50% and p999 latency decrease 40% when `max_batch_size=20` configured.

![](https://cdn.jsdelivr.net/gh/yongman/i@img/picgo/20220921111329.png)

## Performance

We benchmark the cluster with 3 TiKV nodes, 3 Tidis nodes, 1 PD node and 1 TiDB node (for gc). We benchmark the cluster using multiple `memtier-benchmark` processes, with various number of parallel connections. The benchmark result shows the `read` and `write` throughput can be reached `540k ops/s` and `125k op/s`.

![](https://cdn.jsdelivr.net/gh/yongman/i@img/picgo/20220921114120.png)

The latency distribution shows that the latency will increase when the cluster load reached to the limit. The `p9999` latency is incresed obviously when having 1200 parallel connections, up to `47ms`.  

![](https://cdn.jsdelivr.net/gh/yongman/i@img/picgo/20220921115048.png)

We also compare the benchmark result with other `Redis on TiKV` projects (titan and tidis1.0), the result show that `Tidis` has better write performance than titan and tidis1.0.

![](https://cdn.jsdelivr.net/gh/yongman/i@img/picgo/20220921120700.png)

The write thoughput of `Tidis` is almost 2 times of titan and tidis1.0 and the latency are the best all the time.

See more [Benchmark](https://github.com/tidb-incubator/tidis/blob/master/docs/performance.md) details here. 

