# What is OceanBase Database
OceanBase Database is a native distributed relational database. It is developed entirely by Ant Group. OceanBase Database is built on a common server cluster. Based on the Paxos protocol and its distributed structure, OceanBase Database provides high availability and linear scalability. OceanBase Database is not dependent on specific hardware architectures.

## Core features

- Scalable OLTP
   - Linear scalability by adding nodes to the cluster
   - Partition-level leader distribution and transparent data shuffling 
   - Optimized performance for distributed transaction through "table group" technology
   - High concurrency updates on hot row through early lock release (ELR)
   - 80000+ connections per node and unlimited connections in one instance through multi threads and coroutines
   - Prevent silent data corruption (SDC) through multidimensional data consistency checksum
   - No.1 in TPC-C benchmark with 707 million tpmC
- Operational OLAP
   - Process analytical tasks in one engine, no need to migrate data to OLAP engine
   - Analyze large amounts of data on multiple nodes in one OceanBase cluster with MPP architecture
   - Advanced SQL engine with CBO optimizer, distributed execution scheduler and global index
   - Fast data loading through parallel DML, and with only 50% storage cost under compression
   - Broke world record with 15.26 million QphH in TPC-H 30TB benchmark in 2021
- Multi-tenant
   - Create multiple tenants (instances) in one OceanBase cluster with isolated resource and access
   - Multidimensional and transparently scale up/out for each tenant, and scaling up takes effect immediately
   - Database consolidation: multi-tenant and flexible scaling can achieve resource pooling and improve utilization
   - Improve management efficiency and reduce costs without compromising performance and availability

## Quick start
See [Quick start](https://open.oceanbase.com/quickStart) to try out OceanBase Database.

## System architecture

![image.png](https://cdn.nlark.com/yuque/0/2022/png/25820454/1667369873624-c1707034-471a-4f79-980f-6d1760dac8eb.png)

## Roadmap

![image.png](https://cdn.nlark.com/yuque/0/2022/png/25820454/1667369873613-44957682-76fe-42c2-b4c7-9356ed5b35f0.png)

Link: [4.0.0 function list](https://github.com/oceanbase/oceanbase/milestone/3)

## Case study
For our success stories, see [Success stories](https://www.oceanbase.com/en/customer/home).

## Contributing
Your contributions to our code will be highly appreciated. For details about how to contribute to OceanBase, see [Contribute to OceanBase](https://github.com/oceanbase/oceanbase/wiki/Contribute-to-OceanBase).

## Licensing
OceanBase Database is under [MulanPubL - 2.0](http://license.coscl.org.cn/MulanPubL-2.0/#english) license. You can freely copy and use the source code. When you modify or distribute the source code, please follow the MulanPubL - 2.0 license.

## Community

- [oceanbase.slack](https://oceanbase.slack.com/)
- [Forum (Simplified Chinese)](https://ask.oceanbase.com/)
- [DingTalk 33254054 (Simplified Chinese)](https://h5.dingtalk.com/circle/healthCheckin.html?corpId=ding12cfbe0afb058f3cde5ce625ff4abdf6&53108=bb418&cbdbhh=qwertyuiop&origin=1)
- [WeChat (Simplified Chinese)](https://gw.alipayobjects.com/zos/oceanbase/0a69627f-8005-4c46-be1f-aac7a2b85c13/image/2022-03-01/85d42796-4e22-463a-9658-57402d7b9bc3.png)

## Refs

### OceanBase

- OceanBase大赛 | 手摸手带你玩转OceanBase https://zhuanlan.zhihu.com/p/445201899

### Profiling

- 分析IO的工具 ioprof https://bean-li.github.io/ioprof/

依赖的Perl 库安装：

```
yum install perl-Digest-MD5 -y
yum install perl-Thread-Queue -y
```

ioprof 本体下载：

```
gcl https://github.com/intel/ioprof
```

工具使用:

```shell
gcl https://github.com/intel/ioprof.git
cd ioprof
./ioprof.pl -m trace -d /dev/vda1 -r 300 # 开始profiling，生成vda1.tar数据文件
./ioprof.pl -m post -t vda1.tar    # (可选)在终端根据生成的数据文件，展示heatmap进行可视化
./ioprof.pl -m post -t vda1.tar -p # (可选，且需要安装gnuplot和texlive) 在pdf中对数据进行可视化
```
其中, /dev/vda1 是服务器的磁盘设备名, 可以通过`df -h`来判断想要监控哪个磁盘设备。`-r 300`是表示监控接下来300s的这个设备的IO。

如果想要测试大量的顺序读，可以使用如下指令：

```sh
fio --name=seqread --rw=read --bs=1M  --size=5G --runtime=400 --numjobs=10 --direct=1 --group_reporting
```

## read constant
0: 1 1
37500726: 37502915 5
75001452: 75015168 1
112502178: 112509890 2
150002904: 150006562 1
187503630: 187501475 2
225004356: 224993826 1
262505082: 262492770 3
300005808: 299999975 3

0: 1 1
75001452: 75015168 1
150002904: 150006562 1
225004356: 224993826 1
300005808: 299999975 3

## TODO (sort by priority)
1. [ ] prefetch buffer
   1. use two buffers A, B for csv_parser, no need to squaze anymore
   2. when working in B, let A=B and reset B, then start prefetch thread
   3. so, when csv_parser needs the second buffer, it waits (but i think this will never happen), but we will need to write wait.
2. improve sort
3. write own caster
4. read from file's different positions, use 
   
   https://stackoverflow.com/questions/18009287/get-file-content-from-specific-position-to-another-specific-position
5. [ ] modify the constant now tenant has 12G
6. [ ] https://gitlab.com/daniel.langr/cpp11sort/-/tree/master/
7. [ ] better flush buffer strategy

## benchmark
1. 8MB sort buffer 54m7s
2. 2MB sort buffer 49m56s
3. 1MB sort buffer
4. fast csv 44m55s