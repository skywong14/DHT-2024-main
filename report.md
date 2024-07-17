### Report for DHT
### 1.Chord
#### 1.1概述
在Chord算法中，可将节点构成的网络视作一个**加强版**的环。
这个环具有以下特性：
- 环上节点按唯一的64位HashId升序排列
- 在我的实现中，每个节点除了存储自身的数据外，备份且仅备份前驱节点的数据。
- 节点被强制下线时，Maintain进程会修复有问题的fingerTable，successorList和precessorNode，同时释放下线节点后继的备份数据。
- 稍详细的，对于每个节点：
   - 有路由表fingerTable，用于快速查询一个HashId（可能代表Key，也可能代表节点，其具体含义对查询没有影响）可能归属的节点。它保证了查询的复杂度在Log级别。
   - 有successorList，用于记录后继节点。这是整个DHT正确性的保证。
   - 归属自身的数据存储在data中，备份的数据存储在backup中。
   - 存储了predecessorNode，但predecessorNode的正确性不能保证，会需要经常更新。
   - 有一系列读写锁，保证并发时的正确性。
#### 1.2 复杂度
**查询数据**的复杂度的上限由fingerTable确定，每次fingerTable上的跳转可以保证离目标节点的距离会变小，且在下一个节点查询时的距离不小于当前距离的一半。对于64位HashId的Chord网络，查询的跳转次数不超过64次。
**插入和删除数据**的复杂度与查询操作同阶。插入时需要在后继节点上备份，删除时则同样需要在后继节点上删除备份。
**新节点加入网络**包含查询节点的后继与初始化节点。该节点的fingerTable可以在Maintain中逐步构建。

**节点正常退出网络**的复杂度来自移交数据与更新其他节点的fingerTable两部分。移交数据只涉及前驱和两个后继节点，更新其它节点的fingerTable则需要递归处理，但被更新到的节点数依然是log的。

**Maintain维护进程**包含三个部分。对于每个节点，分别需要：1.完善和修复fingerTable。2.完善和修复successorList。3.提醒后继节点它的前驱是自身。一共三个进程。
#### 1.3 稳定性
对于DHT网络来说，只要successorList是正确的（甚至直至需要successorList[0]，即后继节点是正确的），通过有限次Maintain即可获得正确的路由表和前驱。而在某个节点的ForeceQuit后，该节点前驱可以通过的successorList迅速更新得到正确的后继节点。
对于存储的数据来说，只要不出现相邻两个节点同时ForceQuit的极端情况，下线节点的数据都可以快速的从backup中恢复。
### 2.Kademlia
#### 2.1概述
Kademlia


#### 3.关于Go语言和TCP连接
