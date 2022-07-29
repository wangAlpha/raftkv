RaftKV is a simple Golang distributed consistent KV server based on
the raft protocol.

http://github.com/wangAlpha/rafkv

Requires:
  Linux kernel version >= 2.6.28
  Golang >= 1.6 and Python >= 3.7


![image](https://user-images.githubusercontent.com/14357954/175243606-bb1de833-3a00-41eb-9bba-22038f612390.png)

### 简介
一个 Go 实现的内存版分布式 KV 服务器。

## 项目概述
### Raft
  分布式下的容灾一般采用多副本节点的数据复制技术，可以达到分布式容灾，提高服务的可用性，降低单点节点故障等等。
  不过由于 CAP 理论，多节点带来了一致性问题。

  Raft 是一个强领导的基于状态机的集权一致性算法，此部分对外提供功能保证：外部追加的 `log entry`
  一旦 success 成功，便会成为该集群共识。一个总机器 Quorum 数量的集群，至多可容纳 Quorum/2 的节点掉线；
  在网络不稳定的情况下仍然利用各种内部传输的机制，对外提供可靠服务；可以进行持久化，以对抗机器的重启。
#### Raft 层实现
  本部分代码主要位于 `src/raft` 目录
 - 实现 `RequestVote RPC` ，从而实现初始选举、网络掉线之后的选举、以及心跳机制。该lab使得Leader得以尽快选举，并确保每一个term只能有至多一个leader.
 - 实现 `AppendEntries RPC`，使得leader可以不断地尝试各个follower追加log，并尽力最终使得这些log成为共识。一旦某log被过半数的raft所追加，那么这个log就会成为已提交的（commited）；leader会在心跳中向follower表明这一点，并使得网络状态正常的follower得知。raft会针对其自身的已提交log，向与其绑定的server发送apply message.
 - 实现持久化机制，并进行论文 Figure8 逻辑检验（该检验要求raft始终选出正确的leader，即使经历网络掉线、重启等，raft也能就commited log实现共识，切不向server提交不成为共识的任何日志节点），churn检验（该检验要求raft能够尽快地从重启中恢复并向外提供服务）等。
 - 实现 raft 的 `Snapshot` 机制，该机制允许raft在日志过长时，将前面的若干个log整合为一张快照。这要求改写日志的序列号逻辑。由于leader前面的日志丢失，所以可能会出现新follower无法追加前面的log的情况。在这种情况下，leader可以对follower发送install snapshot RPC。该lab与lab 3B实现了较强的联动。

### RaftKV
 本部分代码主要位于 `src/kvraft`
 - Raft 层已经实现了可靠的 `Raft` 集群功能，本部分将抽离出 `DB` 类，结合 `Raft` 层实现一个可靠的 `DB` 集群。
 - 由于网络的不可靠，出现乱序、超时和正在选举的情况下， server 可能会收到两次相同的历史请求。这就需要实现 `linearizable semantics`，这里采用客户端序列号+客户端指令序列号+server保存临时表机制，使得每台client发出的请求序列均满足线性一致性。
 - 当 server 长期运行时，各个节点的Raft Log可能会增长得非常大，会耗费过大的存储资源，故节点应该在特定的时候（当Raft Log达到一定的数量时候）将当前状态存储至 `Snapshot` 中，然后丢弃掉快照前的所有 `Raft Log`。当一个节点重启时，应该先重装快照并读取和重现快照后的 `Raft Log`。

### Multi-Raft
 本部分代码主要位于 `src/shardmaster` 和 `src/shardkv`
#### 需要解决的问题
单个 Raft-Group 在 KV 的场景下存在一些弊端:
 - 单机的存储容量限制了系统的存储容量；
 - 请求量进一步提升时，单机无法处理这些请求（读写请求都由Leader节点处理）。
#### MultiRaft 需要解决的一些核心问题：
 - 数据何如分片。
 - 分片中的数据越来越大，需要分裂产生更多的分片，组成更多 Raft-Group。
 - 分片的调度，让负载在系统中更平均（分片副本的迁移，补全，Leader 切换等等）。
 - 一个节点上，所有的 Raft-Group 复用链接（否则 Raft 副本之间两两建链，链接爆炸了）。
 - 如何处理 stale 的请求（例如 Proposal 和 Apply 的时候，当前的副本不是 Leader、分裂了、被销毁了等等）。
 - Snapshot 如何管理（限制Snapshot，避免带宽、CPU、IO资源被过度占用）。

## Reference
 1. [raft 在线动画演示](http://www.kailing.pub/raft/index.html)
 2. [raft 中英文论文](https://github.com/maemual/raft-zh_cn)
 3. [raft 作者博士论文](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
 4. [Raft 的优化](https://pingcap.com/zh/blog/optimizing-raft-in-tikv)
 5. [Raft 的应用与优化](https://codeantenna.com/a/skgtzCiILK)
 6. [Multi-Raft在TiKV中的实践](https://blog.51cto.com/u_15703183/5449815)
