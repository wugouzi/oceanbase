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

## TODO (sort by priority)
1. [ ] sstable use block instead of row
2. [ ] cast_obj_to_datum
3. [ ] external_sort
4. [ ] 读取数据的时候,block block地读
5. [ ] 读一个buffer数据,先sort,再写回去()
6. [ ] 读了之后,sort,写回去 -> 我们的文件是不同的sorted的数据,可以merge了 (非常不好写)
   1. [ ] 读满内存,sort,写回去
   2. [ ] FILE_BUFFER_SIZE
7. [ ] learn LSM-tree
8. [ ] after read a buffer, sort it directly and write back
9.  [ ] parallel load data
10. [ ] how does ob manage threads?
11. [ ] src/storage/ob_parallel_external_sort.h:513 看一下sstable这个item啥时候回收
