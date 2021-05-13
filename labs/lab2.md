## [6.824](http://nil.csail.mit.edu/6.824/2020/index.html) - Spring 2020

# 6.824 Lab 2: Raft

<!-- ### Part 2A Due: Feb 21 23:59 -->

### Part 2B Due: Feb 28 23:59

### Part 2C Due: Mar 6 23:59

------

### Introduction

This is the first in a series of labs in which you'll build a fault-tolerant key/value storage system. In this lab you'll implement Raft, a replicated state machine protocol. In the next lab you'll build a key/value service on top of Raft. Then you will “shard” your service over multiple replicated state machines for higher performance.

A replicated service achieves fault tolerance by storing complete copies of its state (i.e., data) on multiple replica servers. Replication allows the service to continue operating even if some of its servers experience failures (crashes or a broken or flaky network). The challenge is that failures may cause the replicas to hold differing copies of the data.

Raft organizes client requests into a sequence, called the log, and ensures that all the replica servers see the same log. Each replica executes client requests in log order, applying them to its local copy of the service's state. Since all the live replicas see the same log contents, they all execute the same requests in the same order, and thus continue to have identical service state. If a server fails but later recovers, Raft takes care of bringing its log up to date. Raft will continue to operate as long as at least a majority of the servers are alive and can talk to each other. If there is no such majority, Raft will make no progress, but will pick up where it left off as soon as a majority can communicate again.

In this lab you'll implement Raft as a Go object type with associated methods, meant to be used as a module in a larger service. A set of Raft instances talk to each other with RPC to maintain replicated logs. Your Raft interface will support an indefinite sequence of numbered commands, also called log entries. The entries are numbered with *index numbers*. The log entry with a given index will eventually be committed. At that point, your Raft should send the log entry to the larger service for it to execute.

You should follow the design in the [extended Raft paper](http://nil.csail.mit.edu/6.824/2020/papers/raft-extended.pdf), with particular attention to Figure 2. You'll implement most of what's in the paper, including saving persistent state and reading it after a node fails and then restarts. You will not implement cluster membership changes (Section 6). You'll implement log compaction / snapshotting (Section 7) in a later lab.

You may find this [guide](https://thesquareplanet.com/blog/students-guide-to-raft/) useful, as well as this advice about [locking](http://nil.csail.mit.edu/6.824/2020/labs/raft-locking.txt) and [structure](http://nil.csail.mit.edu/6.824/2020/labs/raft-structure.txt) for concurrency. For a wider perspective, have a look at Paxos, Chubby, Paxos Made Live, Spanner, Zookeeper, Harp, Viewstamped Replication, and [Bolosky et al.](http://static.usenix.org/event/nsdi11/tech/full_papers/Bolosky.pdf)

This lab is due in three parts. You must submit each part on the corresponding due date.

### Collaboration Policy

You must write all the code you hand in for 6.824, except for code that we give you as part of the assignment. You are not allowed to look at anyone else's solution, you are not allowed to look at code from previous years, and you are not allowed to look at other Raft implementations. You may discuss the assignments with other students, but you may not look at or copy anyone else's code, or allow anyone else to look at your code.

Please do not publish your code or make it available to current or future 6.824 students. `github.com` repositories are public by default, so please don't put your code there unless you make the repository private. You may find it convenient to use [MIT's GitHub](https://github.mit.edu/), but be sure to create a private repository.

### Getting Started

If you have done Lab 1, you already have a copy of the lab source code. If not, you can find directions for obtaining the source via git in the [Lab 1 instructions](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html).

We supply you with skeleton code `src/raft/raft.go`. We also supply a set of tests, which you should use to drive your implementation efforts, and which we'll use to grade your submitted lab. The tests are in `src/raft/test_test.go`.

To get up and running, execute the following commands. Don't forget the `git pull` to get the latest software.

```shell
$ cd ~/6.824
$ git pull
...
$ cd src/raft
$ go test
Test (2A): initial election ...
--- FAIL: TestInitialElection2A (5.04s)
        config.go:326: expected one leader, got none
Test (2A): election after network failure ...
--- FAIL: TestReElection2A (5.03s)
        config.go:326: expected one leader, got none
...
$
```

### The code

Implement Raft by adding code to `raft/raft.go`. In that file you'll find skeleton code, plus examples of how to send and receive RPCs.

Your implementation must support the following interface, which the tester and (eventually) your key/value server will use. You'll find more details in comments in `raft.go`.

```go
// create a new Raft server instance:
rf := Make(peers, me, persister, applyCh)

// start agreement on a new log entry:
rf.Start(command interface{}) (index, term, isleader)

// ask a Raft for its current term, and whether it thinks it is leader
rf.GetState() (term, isLeader)

// each time a new entry is committed to the log, each Raft peer
// should send an ApplyMsg to the service (or tester).
type ApplyMsg
```

A service calls `Make(peers,me,…)` to create a Raft peer. The peers argument is an array of network identifiers of the Raft peers (including this one), for use with RPC. The `me` argument is the index of this peer in the peers array. `Start(command)` asks Raft to start the processing to append the command to the replicated log. `Start()` should return immediately, without waiting for the log appends to complete. The service expects your implementation to send an `ApplyMsg` for each newly committed log entry to the `applyCh` channel argument to `Make()`.

`raft.go` contains example code that sends an RPC (`sendRequestVote()`) and that handles an incoming RPC (`RequestVote()`). Your Raft peers should exchange RPCs using the labrpc Go package (source in `src/labrpc`). The tester can tell `labrpc` to delay RPCs, re-order them, and discard them to simulate various network failures. While you can temporarily modify `labrpc`, make sure your Raft works with the original `labrpc`, since that's what we'll use to test and grade your lab. Your Raft instances must interact only with RPC; for example, they are not allowed to communicate using shared Go variables or files.

Subsequent labs build on this lab, so it is important to give yourself enough time to write solid code.

### Part 2A

实现 Raft leader election 和 heartbeat (`AppendEntries` RPCs with no log entries). Run `go test -run 2A` to test your 2A code.


- 阅读 `Figure 2`.
- 实现`RequestVoteArgs`和`RequestVoteReply` structs，修改`Make` 创建一个周期性执行`RequestVote`的线程，在没有leader
- 心跳和超时计数器重制
-  不要同时成为candicated
- The tester requires   <= 10/s 心跳
-  选举需要在5s内完成（包括re-election和脑裂）
- The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds. Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds. Because the tester limits you to 10 heartbeats per second, you will have to use an election timeout larger than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader within five seconds.
-  周期性动作建议使用 `time.Sleep`而不是`time.Timer` `time.Ticker`
-  注意`rf.Kill()`,结束时tester会调用
- `go test -run 2A > out` 调试
- Check your code with `go test -race`, and fix any races it reports. 检查race条件 `go test -race`

Be sure you pass the 2A tests before submitting Part 2A, so that you see something like this:

```shell
$ go test -run 2A
Test (2A): initial election ...
  ... Passed --   4.0  3   32    9170    0
Test (2A): election after network failure ...
  ... Passed --   6.1  3   70   13895    0
PASS
ok      raft    10.187s
$
```

Each "Passed" line contains five numbers; these are the time that the test took in seconds, the number of Raft peers (usually 3 or 5), the number of RPCs sent during the test, the total number of bytes in the RPC messages, and the number of log entries that Raft reports were committed. Your numbers will differ from those shown here. You can ignore the numbers if you like, but they may help you sanity-check the number of RPCs that your implementation sends. For all of labs 2, 3, and 4, the grading script will fail your solution if it takes more than 600 seconds for all of the tests (`go test`), or if any individual test takes more than 120 seconds.

### Part 2B

实现leader和follower的append new log entries，运行 `go test -run 2B`

<span style='color:red'>
Implement the leader and follower code to append new log entries, so that the `go test -run 2B` tests pass.
</span>


- 首先实现 `TestBasicAgree2B()`. 实现 `Start()`, then write the code to send and receive new log entries via `AppendEntries` RPCs, following Figure 2.
- 需要实现 electrion restriction
- 反复选举的bug和选举后的心跳问题
- 可以使用条件变量检查事件的发生
- Do yourself a favor for future labs and write (or re-write) code that's clean and clear. 

The tests for upcoming labs may fail your code if it runs too slowly. You can check how much real time and CPU time your solution uses with the time command. Here's typical output:

```shell
$ time go test -run 2B
Test (2B): basic agreement ...
  ... Passed --   1.6  3   18    5158    3
Test (2B): RPC byte count ...
  ... Passed --   3.3  3   50  115122   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   6.3  3   64   17489    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   4.9  5  116   27838    3
Test (2B): concurrent Start()s ...
  ... Passed --   2.1  3   16    4648    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   8.1  3  111   26996    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  28.6  5 1342  953354  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   3.4  3   30    9050   12
PASS
ok      raft    58.142s

real    0m58.475s
user    0m2.477s
sys     0m1.406s
$
```

### Part 2C

重启的持久化

<span style='color:red'>
If a Raft-based server reboots it should resume service where it left off. This requires that Raft keep persistent state that survives a reboot. The paper's Figure 2 mentions which state should be persistent.
</span>

定期写磁盘，使用 `Persiter`， 能够使用`Persister`初始化`Persister`恢复状态，使用`ReadRaftState` 和`SaveRaftState`方法

完成`persist()`和`readPersist` 增加保存和恢复persisten state代码，可以使用encode; `gob` encoder 需要大写的结构体字域名称

改变状态的地方加入 `persist()`

需要周期性丢弃old entries以保证内存

- 可能需要优化备份 nextIndex， You will probably need the optimization that backs up nextIndex by more than one entry at a time. Look at the [extended Raft paper](http://nil.csail.mit.edu/6.824/2020/papers/raft-extended.pdf) starting at the bottom of page 7 and top of page 8 (marked by a gray line). The paper is vague about the details; you will need to fill in the gaps, perhaps with the help of the 6.824 Raft lectures.
- 总之的测试时间在4分钟左右

Your code should pass all the 2C tests (as shown below), as well as the 2A and 2B tests.

```shell
$ go test -run 2C
Test (2C): basic persistence ...
  ... Passed --   7.2  3  206   42208    6
Test (2C): more persistence ...
  ... Passed --  23.2  5 1194  198270   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   3.2  3   46   10638    4
Test (2C): Figure 8 ...
  ... Passed --  35.1  5 9395 1939183   25
Test (2C): unreliable agreement ...
  ... Passed --   4.2  5  244   85259  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  36.3  5 1948 4175577  216
Test (2C): churn ...
  ... Passed --  16.6  5 4402 2220926 1766
Test (2C): unreliable churn ...
  ... Passed --  16.5  5  781  539084  221
PASS
ok      raft    142.357s
$
```

Please post questions on [Piazza](http://piazza.com/).