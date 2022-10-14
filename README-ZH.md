# Tidis

[![CI](https://github.com/tidb-incubator/tidis/actions/workflows/ci.yml/badge.svg)](https://github.com/tidb-incubator/tidis/actions/workflows/ci.yml)
[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README-ZH.md)

`Tidis` 是 TiKV 的服务层，在其基础上建设一个兼容 redis 协议的分布式存储服务。目前 Tidis 已经实现了对多种 Redis 数据类型（string/hash/list/set/sortedset）的支持。

为了更加强劲的性能和更低的延迟，同时为了增加更多诸如 Lua 脚本，TLS 连接，锁优化等重要功能的支持，我们重新设计了 `Tidis` 并使用 `Rust` 语言实现。

## 背景

Redis 是一个被很多公司和项目使用的支持多种数据结构的内存数据存储系统。虽然它在缓存和数据存储方面已经是一个非常优秀的解决方案，但它目前依然存在一些限制：

- 内存成本高，不适合存储大量的数据
- 不易于管理，且对大数据集的扩容不够友好
- 很多应用场景仅依赖于协议和应用程序接口，并不追求极致的性能
- 不支持全局事务
- 没有强数据持久化保证

我们相信，在低成本的存储介质基础上建立一个兼容 Redis 的分布式存储服务是一个普遍的诉求。从实际情况看，我们也发现已经有 `ssdb`、`pika`、`kvrocks` 等等流行的产品基于这种想法建立。然而他们都是存算一体架构，这意味着他们都需要实现数据副本、高可用、分片、扩展等非常复杂的基本能力。这不仅难以实现，而且运维起来也十分复杂。

考虑到这些复杂性，我们采取了不同的方案：在 [TiKV](https://tikv.org/) 的基础上建立一个轻量的计算服务层。`TiKV` 是一个优秀的可拥有构建多样化存储系统的基石，它已具备强大的分布式存储系统基本能力，如高可用、数据分片、数据扩展、全局事务等等。基于此点，我们只需要实现计算层来提供易用的数据模型和计算能力。

当然，我们并不是第一个想到 Redis on TiKV 方案的人。`Titan` 和 `Tidis 1.0` 都是以类似的架构进行设计并实现的。它们过去都很受欢迎并且拥有了自己的社区。遗憾的是它们目前仍然没有实现一些 Redis 用户期待的常用功能。因此我们相信一款功能更完整的替代项目会得到社区的广泛关注。因此，我们决定在 Tidis 1.0 的基础上，重新设计和实现下一代 Tidis。除了性能优化之外我们开始实现一些过去缺失但对应用开发者非常有帮助的功能，如 Lua 脚本、TLS/SSL、锁优化、一阶段提交、异步提交等等许多其他的改进。

## 特点

* 多协议接入
* 线性扩展能力
* 存算分离
* 高可用、高持久性和强一致性### 
* 全局事务

## 架构

*Tidis 架构*

![architecture](https://raw.githubusercontent.com/tidb-incubator/tidis/master/docs/tidis-arch.png)

*TiKV 架构*

![](https://tikv.org/img/basic-architecture.png)
- Placement Driver (PD): PD 是 TiKV 系统的大脑，它管理着关于节点、存储、区域映射的元数据，并做出数据放置和负载平衡的决定。PD 将会定期检查副本约束，自动平衡负载和数据。
- Node: 集群中的一个物理节点。在每个节点内，有一个或多个 Store，而在一个 Store 内有多个 Region。
- Store: 每个 Store 中存在一个 RocksDB 实例负责数据的持久化。
- Region: 其对应于存储中的一个数据范围，并且是键值数据移动的基本单位。 每个 Region 将会被复制到多个节点，这样的一组副本被称为一个 Raft 组，而一个 Region 中的副本被称为一个 Peer。

## 运行

- 使用 `TiUP` 启动 `PD`， `TiKV` 和一个 `TiDB` 实例 (用于触发垃圾回收)， 具体启动方法可以参考[官方指引](https://docs.pingcap.com/zh/tidb/stable/production-deployment-using-tiup) 。

- 构建 Tidis 服务端

```
make release
```

- 启动 Tidis 服务端

```
tidis-server --config config.toml
```

你可以使用以下示例配置。

``` toml
[server]
listen = "0.0.0.0"
port = 6379                               # disable tcp port if set to 0
tls_listen = "0.0.0.0"
tls_port = 6443                           # disable tls if tls_port set to 0
tls_key_file = ""                         # disable tls if key or cert file not set
tls_cert_file = ""
tls_auth_client = false                   # tls_ca_cert_file must be specified if tls_auth_client is true
tls_ca_cert_file = "path/ca.crt"
pd_addrs = "127.0.0.1:2379"               # PD addresses of the TiKV cluster
instance_id = "1"                         # instance_id can be used as tenant identifier
prometheus_listen = "0.0.0.0"
prometheus_port = 8080
log_level = "info"
log_file = "tidis.log"

[backend]
use_txn_api = true                        # use transaction api for full api supported
use_async_commit = true                   # try to use async commit in tikv
try_one_pc_commit = true                  # try to use one pc commit
use_pessimistic_txn = true                # use optimistic transaction mode
local_pool_number = 4                     # localset pool number for handle connections
txn_retry_count = 10                      # transaction retry count
txn_region_backoff_delay_ms = 2           # transaction region error backoff base delay time
txn_region_backoff_delay_attemps = 2      # transaction region error backoff retry max attempts
txn_lock_backoff_delay_ms = 2             # transaction lock error backoff base delay time
txn_lock_backoff_delay_attemps = 5        # transaction lock error backoff retry max attempts
```

- 运行客户端

对于 redis 协议，你可以使用官方的客户端工具比如：`redis-cli`。

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

## 已支持命令

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

## 运行端到端测试

你可以使用如下命令运行 test 目录中提供的测试工具运行所有已支持的命令：

```
python3 test_helper.py [--ip ip] [--port 6379]
```

## TLS/SSL 支持

TLS/SSL 加密是安全的必要条件，特别是在 AWS、GCP 或 Azure 云等公共访问环境中提供云服务。

`Tidis` 支持 `单向` 和 `双向` 的 TLS 认证握手。 在 `单向` 认证中， 你可以使用 CA 证书和密码进行安全传输和认证。 如果你选择 `双向` 认证， 则可以基于客户端侧的证书和密钥安全地使用 CA 证书而不再需要密码。

### 1. 生成 TLS 证书和密钥用于测试

``` bash
#!/bin/bash

# Generate some test certificates which are used by the regression test suite:
#
#   tests/tls/ca.{crt,key}          Self signed CA certificate.
#   tests/tls/redis.{crt,key}       A certificate with no key usage/policy restrictions.
#   tests/tls/client.{crt,key}      A certificate restricted for SSL client usage.
#   tests/tls/server.{crt,key}      A certificate restricted for SSL server usage.

generate_cert() {
    local name=$1
    local cn="$2"
    local opts="$3"

    local keyfile=tests/tls/${name}.key
    local certfile=tests/tls/${name}.crt

    [ -f $keyfile ] || openssl genrsa -out $keyfile 2048
    openssl req \
        -new -sha256 \
        -subj "/O=Redis Test/CN=$cn" \
        -key $keyfile | \
        openssl x509 \
            -req -sha256 \
            -CA tests/tls/ca.crt \
            -CAkey tests/tls/ca.key \
            -CAserial tests/tls/ca.txt \
            -CAcreateserial \
            -days 365 \
            $opts \
            -out $certfile
}

mkdir -p tests/tls
[ -f tests/tls/ca.key ] || openssl genrsa -out tests/tls/ca.key 4096
openssl req \
    -x509 -new -nodes -sha256 \
    -key tests/tls/ca.key \
    -days 3650 \
    -subj '/O=Redis Test/CN=Certificate Authority' \
    -out tests/tls/ca.crt

cat > tests/tls/openssl.cnf <<_END_
[ server_cert ]
keyUsage = digitalSignature, keyEncipherment
nsCertType = server

[ client_cert ]
keyUsage = digitalSignature, keyEncipherment
nsCertType = client
_END_

generate_cert server "Server-only" "-extfile tests/tls/openssl.cnf -extensions server_cert"
generate_cert client "Client-only" "-extfile tests/tls/openssl.cnf -extensions client_cert"
generate_cert redis "Generic-cert"
```

### 2. 服务端和客户端配置

- 服务端

``` toml
tls_listen = "0.0.0.0"
tls_port = 6443
tls_key_file = "path/server.key"
tls_cert_file = "path/server.crt"
tls_auth_client = false           # tls_ca_cert_file must be specified if tls_auth_client is true
tls_ca_cert_file = "path/ca.crt"
```

- 客户端

`tls_auth_client` 被置为 `false`， 客户端将会使用根 CA 证书

``` shell
./src/redis-cli --tls --cacert ./tests/tls/ca.crt
```

`tls_auth_client` 被置为 `true`， 客户端必须配置证书和密钥文件用于服务端校验

``` shell
./src/redis-cli --tls  --cert ./tests/tls/client.crt --key ./tests/tls/client.key --cacert ./tests/tls/ca.crt
```

## 全局事务

得益于 `TiKV` 集群的全局事务机制，`Tidis` 可以轻松地支持全局事务。就像使用一个单实例的 `Redis` 一样使用 `MULTI/EXEC/DISCARD` 命令，同时还不需要关心使用 `Redis Cluster` 时会遇到的 `CROSSSLOT` 错误。

`Tidis` 支持 `乐观` 和 `悲观` 两种事务模型。

悲观事务适用于对某些热点数据有很多并发的写操作的场景，除此之外，乐观事务往往能取得更好的性能。

具体可以参考 TiDB 文档中的 [乐观事务](https://docs.pingcap.com/tidb/dev/optimistic-transaction) 和 [悲观事务](https://docs.pingcap.com/tidb/dev/pessimistic-transaction) 。

此外， `1pc` 和 `async commit` 选项在大多数场景中对于取得更好的性能是有益的。 具体可参考 PingCAP 相关博客：[AsyncCommit, the Accelerator for Transaction Commit in TiDB 5.0](https://www.pingcap.com/blog/async-commit-the-accelerator-for-transaction-commit-in-tidb-5-0/) 。

## Lua 脚本

`Tidis` 使用 `mlua` 库来转译 lua 脚本。 我们可以使用 `EVAL/EVALSHA` 来执行在全局事务支持下的 lua 脚本。同样我们不会遇到使用 `Redis Cluster` 时会遇到的 `CROSSSLOT` 错误。

Lua 脚本将会在一个新的事务上下文中运行，脚本中所有的读、写操作都是可以保证原子性。

所有 lua 脚本的端到端测试用例位于 [test/test_lua.py](https://github.com/tidb-incubator/tidis/blob/master/test/test_lua.py) 。

## 异步删除

对于有大量数据项的大集合键，删除可能是一个非常耗时的操作。启用异步删除功能可以将删除异步化，从而大大降低该类操作的耗时。

大键的删除任务将会在后台运行，同时立即响应用户无需等待后台任务完成。我们会维护同一个键的多个版本以保证并发安全。如果旧版本处于等待被删除的状态，则该键的新版本则会基于该键的最后一个版本单调递增，否则键的新版本将会从 0 开始。

对于有数千个元素的大键的删除，耗时从几秒钟减少到了几毫秒。

|      type      |    hash    |    list    |    set     | sorted set |
| :------------: | :--------: | :--------: | :--------: | :--------: |
| sync deletion  | 1.911778 s | 2.047429 s | 2.145035 s | 4.892823 s |
| async deletion | 0.005159 s | 0.004694 s | 0.005370 s | 0.005403 s |

## Super batch 支持

启用 super batch 将会有显著的性能提升，同时你也可以基于实际的负载进行参数调整。

Super batch 是一个可以减少到 TiKV 的请求往返次数的功能。它默认是启用的，同时可以通过在配置文件中设置 `allow_batch=false` 来禁用，并且还有更多的配置选项可用于更进一步的参数调整。`max_batch_wait_time` 是一个批次中能够允许的最大等待时间，`max_batch_size` 是一个批次中能够允许的最大键数量。`overload_threshold` 是后端 TiKV 服务器负载的阈值，如果负载高于这个值，批处理请求将被立即发送。

你可以根据你的实际数据负载来调整参数。在下面的测试中，我们设置 `max_batch_wait_time` 为 10ms，`overload_threshold` 为 0，`max_batch_size` 不等。测试结果显示，当 `max_batch_size=20` 时，吞吐量提升了 50%，p999 延迟降低了 40%。

![](https://cdn.jsdelivr.net/gh/yongman/i@img/picgo/20220921111329.png)

## 性能

我们在 3 个 TiKV 节点，3 个 Tidis 节点，1 个 PD 节点和 1 个 TiDB 节点（用于垃圾回收）的集群拓扑上使用多个 `memtier-benchmark` 进程在不同的并发量下对集群进行了基准测试。基准测试结果显示，最大的`读`和`写`吞吐分别为 `540k ops/s` 和 `125k ops/s`。

![](https://cdn.jsdelivr.net/gh/yongman/i@img/picgo/20220921114120.png)

延迟分布显示，当集群负载达到一定限度时，延迟会增加。在 1200 个并发连接的情况下，`p9999` 延迟明显增加，达到 `47ms`。

![](https://cdn.jsdelivr.net/gh/yongman/i@img/picgo/20220921115048.png)

我们还将基准结果与其他类 `Redis on TiKV` 项目（Titan 和 Tidis 1.0）进行了比较，结果显示，`Tidis` 比 Titan 和 Tidis 1.0 具有更高的写吞吐。

![](https://cdn.jsdelivr.net/gh/yongman/i@img/picgo/20220921120700.png)

`Tidis` 的写入吞吐几乎是 Titan 和 Tidis 1.0 的两倍，而且延迟总是低于他们。

点击 [基准测试](https://github.com/tidb-incubator/tidis/blob/master/docs/performance.md) 了解更多。

## 许可

Tidis 适用于 Apache-2.0 许可. 点击 [程序许可](./LICENSE) 了解更多。

## 致谢

* 感谢 [PingCAP](https://github.com/pingcap) 提供的 [TiKV](https://github.com/pingcap/tikv) 和 [PD](https://github.com/pingcap/pd) 等强大的组件。
